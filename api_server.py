import uvicorn
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from typing import List, Optional
from database import Database
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Medical Data API")
db = Database()

class PatientQuery(BaseModel):
    doctor_id: str
    conditions: List[str]

# class Name(BaseModel):
#     use: str
#     family: str
#     given: List[str]
#     prefix: List[str]

class Patient(BaseModel):
    id: str
    resourceType: str
    name: List[dict]
    gender: Optional[str] = None
    birthDate: Optional[str] = None
    conditions: List[str] = []

@app.get("/doctors", response_model=List[str])
async def get_doctors():
    try:
        doctors = await db.get_all_doctors()
        return doctors
    except Exception as e:
        logger.error(f"Error fetching doctors: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/conditions", response_model=List[str])
async def get_conditions():
    try:
        conditions = await db.get_all_conditions()
        return conditions
    except Exception as e:
        logger.error(f"Error fetching conditions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/patients", response_model=List[Patient])
async def get_patients(query: PatientQuery = Body(...)):
    try:
        patients = await db.get_patients_by_doctor_and_conditions(query.doctor_id, query.conditions)
        return patients
    except Exception as e:
        logger.error(f"Error fetching patients: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/patient/{patient_id}", response_model=Optional[Patient])
async def get_patient(patient_id: str):
    try:
        patient = await db.get_patient(patient_id)
        if patient is None:
            raise HTTPException(status_code=404, detail="Patient not found")

        conditions = await db.get_patient_conditions(patient_id)
        patient['conditions'] = conditions

        return Patient(**patient)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching patient: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)