# Medical Data Processing System

This project is a medical data processing system that periodically fetches medical data, stores it in a database, 
and provides an API for doctors to access patient information.

## Setup

1. Clone the repository:
   ```
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Running the Application

1. Start the data fetching process:
   ```
   python main.py
   ```
   This will start fetching data periodically and storing it in the database.

2. In a separate terminal, start the API server:
   ```
   python api_server.py
   ```
   The API will be available at `http://localhost:8000`.

## API Endpoints

- GET `/doctors`: Get all doctors
- GET `/conditions`: Get all conditions
- POST `/patients`: Get patients by doctor and conditions
- GET `/patient/{patient_id}`: Get a specific patient


You can test the API using curl, Postman, or any HTTP client. 
Examples from the tests that I made using postman are below:

Postman:
GET `http://localhost:8000/doctors`
GET `http://localhost:8000/conditions`
GET `http://localhost:8000/patient/736a19c8-eea5-32c5-67ad-1947661de21a`
POST `http://localhost:8000/patients` with body:
```
{
    "doctor_id": "us-npi|9999968891",
    "conditions": ["105531004", "5251000175109"]
}
