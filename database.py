import sqlite3
import aiosqlite
import json
import logging
from typing import Dict, Any, List, Tuple, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_name: str = 'medical_data.db'):
        self.db_name = db_name
        self.init_db()

    def init_db(self) -> None:
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS patients (
                id TEXT PRIMARY KEY,
                data JSON
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS encounters (
                id TEXT PRIMARY KEY,
                patient_id TEXT,
                doctor_id TEXT,
                data JSON,
                FOREIGN KEY (patient_id) REFERENCES patients (id)
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS conditions (
                id TEXT PRIMARY KEY,
                patient_id TEXT,
                encounter_id TEXT,
                code TEXT,
                data JSON,
                FOREIGN KEY (patient_id) REFERENCES patients (id),
                FOREIGN KEY (encounter_id) REFERENCES encounters (id)
            )
            ''')

            conn.commit()
            logger.info("Database initialized")
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")
        finally:
            if conn:
                conn.close()

    async def upsert_patient(self, patient_data: Dict[str, Any]) -> None:
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute('''
                INSERT OR REPLACE INTO patients (id, data)
                VALUES (?, ?)
                ''', (patient_data['id'], json.dumps(patient_data)))
                await db.commit()
            # logger.debug(f"Patient {patient_data['id']} upserted")
        except Exception as e:
            logger.error(f"Error upserting patient: {e}")

    async def upsert_encounter(self, encounter_data: Dict[str, Any]) -> None:
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute('''
                INSERT OR REPLACE INTO encounters (id, patient_id, doctor_id, data)
                VALUES (?, ?, ?, ?)
                ''', (encounter_data['id'], encounter_data['subject']['reference'].split('/')[-1],
                      encounter_data['participant'][0]['individual']['reference'].split('/')[-1],
                      json.dumps(encounter_data)))
                await db.commit()
            # logger.debug(f"Encounter {encounter_data['id']} upserted")
        except Exception as e:
            logger.error(f"Error upserting encounter: {e}")

    async def upsert_condition(self, condition_data: Dict[str, Any]) -> None:
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute('''
                INSERT OR REPLACE INTO conditions (id, patient_id, encounter_id, code, data)
                VALUES (?, ?, ?, ?, ?)
                ''', (condition_data['id'], condition_data['subject']['reference'].split('/')[-1],
                      condition_data['encounter']['reference'].split('/')[-1],
                      condition_data['code']['coding'][0]['code'],
                      json.dumps(condition_data)))
                await db.commit()
            # logger.debug(f"Condition {condition_data['id']} upserted")
        except Exception as e:
            logger.error(f"Error upserting condition: {e}")

    async def get_table_contents(self, table_name: str, limit: int = 10) -> Tuple[List[str], List[Tuple]]:
        try:
            async with aiosqlite.connect(self.db_name) as db:
                cursor = await db.execute(f'SELECT * FROM {table_name} LIMIT {limit}')
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
            return columns, rows
        except Exception as e:
            logger.error(f"Error getting table contents: {e}")
            return [], []

    async def get_all_doctors(self) -> List[str]:
        try:
            async with aiosqlite.connect(self.db_name) as db:
                cursor = await db.execute('SELECT DISTINCT doctor_id FROM encounters')
                rows = await cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Error getting all doctors: {e}")
            return []

    async def get_all_conditions(self) -> List[str]:
        try:
            async with aiosqlite.connect(self.db_name) as db:
                cursor = await db.execute('SELECT DISTINCT code FROM conditions')
                rows = await cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Error getting all conditions: {e}")
            return []

    async def get_patient(self, patient_id: str) -> Optional[Dict[str, Any]]:
        try:
            async with aiosqlite.connect(self.db_name) as db:
                cursor = await db.execute('SELECT data FROM patients WHERE id = ?', (patient_id,))
                row = await cursor.fetchone()
                if row:
                    patient_data = json.loads(row[0])
                    # Fetch conditions for this patient
                    conditions = await self.get_patient_conditions(patient_id)
                    patient_data['conditions'] = conditions
                    return patient_data
                return None
        except Exception as e:
            logger.error(f"Error getting patient: {e}")
            return None

    async def get_patient_conditions(self, patient_id: str) -> List[str]:
        try:
            async with aiosqlite.connect(self.db_name) as db:
                cursor = await db.execute('SELECT DISTINCT code FROM conditions WHERE patient_id = ?', (patient_id,))
                rows = await cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Error getting patient conditions: {e}")
            return []

    async def get_patients_by_doctor_and_conditions(self, doctor_id: str, conditions: List[str]) -> List[dict]:
        try:
            query = '''
            SELECT DISTINCT p.data
            FROM patients p
            JOIN encounters e ON p.id = e.patient_id
            LEFT JOIN conditions c ON p.id = c.patient_id
            WHERE e.doctor_id = ?
            '''
            params = [doctor_id]

            if conditions:
                placeholders = ','.join('?' for _ in conditions)
                query += f' AND c.code IN ({placeholders})'
                params.extend(conditions)

            async with aiosqlite.connect(self.db_name) as db:
                cursor = await db.execute(query, params)
                rows = await cursor.fetchall()

            patients = []
            for row in rows:
                patient_data = json.loads(row[0])
                patient_conditions = await self.get_patient_conditions(patient_data['id'])
                patient_data['conditions'] = patient_conditions
                patients.append(patient_data)

            return patients
        except Exception as e:
            logger.error(f"Error getting patients by doctor and conditions: {e}")
            return []

