use std::{env, error::Error, io::Cursor, path::PathBuf};

use axum::{
    extract::State, response::IntoResponse, routing::{get, post}, Json, Router
};
use serde_json::json;
use log::{error, trace};
use polars::prelude::*;
use tokio::sync::Mutex;
use tracing_subscriber::fmt::format;

#[derive(Clone, Debug)]
struct AppState {
    // A "global source of truth" dataframe
    df: Option<DataFrame>,
    output_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::fmt::init();

    // Initialize the app state
    let mut app_state = AppState {
        df: None,
        output_file: None,
    };

    // Check if the user has provided a CSV file
    let args: Vec<String> = env::args().collect();
    for arg in args {
        if arg.ends_with(".csv") {
            let csv_file = arg;

            // Use Polars to read the CSV
            let df = CsvReader::new(Cursor::new(csv_file.clone())).finish().unwrap();

            // Update the app state
            app_state.df = Some(df);
            app_state.output_file = Some(PathBuf::from(csv_file.clone()));
            
            break;
        }
    }

    // Check for IP-related arguments
    let mut expose_ip = String::from("0.0.0.0");
    let mut port = 3000;
    let args: Vec<String> = env::args().collect();
    for (i, arg) in args.iter().enumerate() {
        if arg == "--local" {
            expose_ip = String::from("127.0.0.1");
        }

        if arg == "--port" {
            port = args[i + 1].parse::<u16>().unwrap();
        }
    }

    // Create a reference to the app state (will be shared across threads/tokio tasks, so needs to be thread safe)
    let state_ref = Arc::new(Mutex::new(app_state));

    // Build router
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/", get(root))
        // `POST /collate` goes to `collate`
        .route("/collate", post(collate))
        // `POST /aggregate` goes to `aggregate`
        .route("/aggregate", post(aggregate))
        // Add the app state to the router
        .with_state(state_ref);

    // Create a listener
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", expose_ip, port))
        .await
        .unwrap();

    tracing::debug!("listening on {}", listener.local_addr().unwrap());

    // Serve app with hyper
    axum::serve(listener, app).await.unwrap();
}

// Health check, essentially
async fn root() -> impl IntoResponse {
    trace!("Root endpoint (GET /) called. Returning operational status.");
    
    Json(json!({
        "status": "operational"
    }))
}

// handler that accepts a POST request with a CSV payload and returns a JSON response
#[axum_macros::debug_handler]
async fn collate(State(state): State<Arc<Mutex<AppState>>>, body: String) -> impl IntoResponse {
    trace!("Collating message: {:?}", body);

    // Convert the body into a vector of bytes
    let body_bytes = body.as_bytes();

    // Use Polars to read the CSV
    let mut df = CsvReader::new(Cursor::new(body_bytes)).finish().unwrap();

    // Acquire a lock on the app state within a scope
    let output_csv_text;
    let output_file;
    {
        let mut state = state.lock().await;

        // Set the output file
        output_file = state.output_file.clone();

        // Get the current state
        match state.df.as_ref() {
            Some(df) => {
                // Concatenate the current state with the new DataFrame
                let new_df = match df.vstack(df) {
                    Ok(df) => df,
                    Err(e) => {
                        error!("Error concatenating DataFrames: {:?}", e);
                        return Json(json!({
                            "status": "error",
                            "message": e.to_string()
                        }));
                    }
                };

                // Update the app state
                state.df = Some(new_df);

                output_csv_text = get_df_as_csv(state.df.as_mut().unwrap(), true);

                // Print the DataFrame
                trace!("Concatted. New state:\n{:?}", state.df.as_ref().unwrap());
            },
            None => {
                // If the current state is None, set it to the new DataFrame (don't need to concat!)
                state.df = Some(df.clone());
                
                output_csv_text = get_df_as_csv(state.df.as_mut().unwrap(), true);

                trace!("Brand new, no concat was needed. New state:\n{:?}", state.df.as_ref().unwrap());
            }
        };
    }

    // Directly append the new DataFrame to the output file (if it has been set)
    let mut wrote_to_file = String::from("no");
    if let Some(output_file) = &output_file {
        append_df_to_csv(&mut df, output_file).await.unwrap();
        wrote_to_file = format!("yes: {:?}", output_file);
    }

    Json(json!({
        "status": "success",
        "wrote_to_file": wrote_to_file,
        "csv_string": output_csv_text
    }))
}


#[derive(Debug, Clone)]
enum AggregateOperation {
    Sum,
    Mean,
}

#[inline(always)]
fn group_by_sum(df: &DataFrame, key: &str) -> PolarsResult<DataFrame> {
    let mut summed = df.group_by([key])?
    .sum()?;

    let mut out = summed.clone();

    for col in summed.get_column_names() {
        if col.to_string().ends_with("_sum") {
            let new_name = col.to_string().replace("_sum", "");

            let proper_name = PlSmallStr::from(new_name.clone());

            // Rename the column
            out.rename(col, proper_name)?;
        }
    }

    Ok(out)
}

#[inline(always)]
fn group_by_mean(df: &DataFrame, key: &str) -> PolarsResult<DataFrame> {
    Err(PolarsError::ComputeError("Mean is not supported yet".into()))
}

// handler that accepts a POST request with a CSV payload, updates the state according to keys, and returns the updated DataFrame as a CSV string
#[axum_macros::debug_handler]
async fn aggregate(State(state): State<Arc<Mutex<AppState>>>, body: String) -> impl IntoResponse {
    trace!("Aggregating message: {:?}", body);

    // Convert the body into a vector of bytes
    let body_bytes = body.as_bytes();

    // Use Polars to read the CSV
    let mut df = CsvReader::new(Cursor::new(body_bytes)).finish().unwrap();

    // HACK: Only support sum for now
    let operation = AggregateOperation::Sum;

    // Acquire a lock on the app state within a scope
    let output_csv_text;
    let output_file;
    {
        let mut state = state.lock().await;

        // Set the output file
        output_file = state.output_file.clone();

        // Get the current state
        match state.df.as_ref() {
            Some(state_df) => {
                // Get the first column header
                let key = df.get_columns()[0].name().to_string();

                // Concatenate the current state with the new DataFrame
                let cat_df = match state_df.vstack(&df) {
                    Ok(df) => df,
                    Err(e) => {
                        error!("Error concatenating DataFrames: {:?}", e);
                        return Json(json!({
                            "status": "error",
                            "message": e.to_string()
                        }));
                    }
                };

                // Print the DataFrame
                trace!("Aggregated. New state:\n{:?}", cat_df);

                // Update the DataFrame according to the aggregate operation joining on the first column value 
                let updated_df = match operation {
                    AggregateOperation::Sum => match group_by_sum(&cat_df, key.as_str()) {
                        Ok(df) => df,
                        Err(e) => {
                            error!("Error aggregating DataFrame: {:?}", e);
                            return Json(json!({
                                "status": "error",
                                "message": e.to_string()
                            }));
                        }
                    },
                    AggregateOperation::Mean => match group_by_mean(&cat_df, key.as_str()) {
                        Ok(df) => df,
                        Err(e) => {
                            error!("Error aggregating DataFrame: {:?}", e);
                            return Json(json!({
                                "status": "error",
                                "message": e.to_string()
                            }));
                        }
                    }
                };
                
                // Update the app state
                state.df = Some(updated_df);

                output_csv_text = get_df_as_csv(state.df.as_mut().unwrap(), true);

                // Print the DataFrame
                trace!("Concatted. New state:\n{:?}", state.df.as_ref().unwrap());
            },
            None => {
                // If the current state is None, set it to the new DataFrame (don't need to concat or do any aggregation!)
                state.df = Some(df.clone());
                
                output_csv_text = get_df_as_csv(state.df.as_mut().unwrap(), true);

                trace!("Brand new, no concat was needed. New state:\n{:?}", state.df.as_ref().unwrap());
            }
        };
    }

    // Directly append the new DataFrame to the output file (if it has been set)
    let mut wrote_to_file = String::from("no");
    if let Some(output_file) = &output_file {
        append_df_to_csv(&mut df, output_file).await.unwrap();
        wrote_to_file = format!("yes: {:?}", output_file);
    }

    Json(json!({
        "status": "success",
        "wrote_to_file": wrote_to_file,
        "csv_string": output_csv_text
    }))
}


    // Append a DataFrame to a CSV file. If it doesn't exist, create it.
async fn append_df_to_csv(df: &mut DataFrame, output_file: &PathBuf) -> Result<(), Box<dyn Error>> {
    let mut file = std::fs::File::create(output_file)?;

    CsvWriter::new(&mut file).include_header(false).finish(df)?;

    Ok(())
}


// Get a DataFrame as a CSV string
fn get_df_as_csv(df: &mut DataFrame, include_header: bool) -> String {
    let mut csv_bytes = Vec::new();

    match CsvWriter::new(&mut csv_bytes).include_header(include_header).finish(df) {
        Ok(_) => (),
        Err(e) => {
            error!("Error writing DataFrame to CSV: {:?}", e);
            return String::new();
        }
    }

    String::from_utf8(csv_bytes).unwrap()
}
