# Data Collator

A simple HTTP service for collecting and aggregating CSV data with persistent storage.

## Overview

Data Collator is a lightweight Rust service built with Axum that provides:

- An HTTP API for collecting CSV data
- Automatic aggregation of incoming data with existing datasets
- Optional persistent storage to a specified CSV file

This tool is useful for data collection scenarios where you need to gather CSV data from multiple sources or over time and maintain a consolidated dataset. We have used it to simplify data collection from many nodes that are each running relatively simple shell scripts.

## Build

### Prerequisites

- Rust and Cargo (latest stable version)
- Internet access (so that dependencies can be built)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/adamweingram/data_collator.git
cd data_collator

# Build the project
cargo build --release
```

## Usage

### Starting the Service

```bash
# Run the binary (without persistent storage)
./target/release/data_collator

# Run the binary (with persistent storage)
./target/release/data_collator [optional_output.csv]

# Run on localhost only (127.0.0.1) rather than all interfaces (0.0.0.0)
./target/release/data_collator --local

# Run on a specific port (default is 3000)
./target/release/data_collator --port 4242

# Combine options
./target/release/data_collator output.csv --local --port 4242
```

By default, the service will start on all network interfaces (`0.0.0.0:3000`). Use the `--local` flag to restrict it to localhost only.

### API Endpoints

#### GET /

Check if the service is running.

**Response:**
```json
{
  "status": "operational"
}
```

#### POST `/collate`

Submit CSV data to be collated with the existing dataset.

> [!CAUTION]
> The very first row in every CSV sent will be interpreted as the header! Make sure you take this into account to avoid data loss.

**Request Body:**
Raw CSV data as text with a header taking up the first row.

**Response:**
```json
{
  "status": "success",
  "wrote_to_file": "yes: \"output.csv\"",
  "csv_string": "CSV content of the current dataset"
}
```

#### POST `/aggregate`

Submit CSV data to be aggregated with the existing dataset. Right now, the aggregation operation is sum. In a future version, it will be possible to specify the desired operation, but this is not yet implemented.

> [!CAUTION]
> As with `/collate`, the very first row in every CSV sent will be interpreted as the header! Make sure you take this into account to avoid data loss.

**Request Body:**
Raw CSV data as text with a header taking up the first row.

**Response:**
```json
{
  "status": "success",
  "wrote_to_file": "yes: \"output.csv\"",
  "csv_string": "CSV content of the current dataset"
}

### Examples

#### Submit data using curl

```bash
# Submit some CSV data
curl -X POST http://localhost:3000/collate -d "column1,column2\nvalue1,value2"

# Aggregate some CSV data
curl -X POST http://localhost:3000/aggregate -d "column1,column2\nvalue1,value2"
```