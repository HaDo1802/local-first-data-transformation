import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Database connection info
load_dotenv()

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME")
}

# Map folders to schemas
SOURCE_MAP = {
    "source_crm": "raw_crm",
    "source_erp": "raw_erp"
}

DATA_DIR = "/Users/hado/Desktop/Career/Coding/Data Engineer /Project/SQL_Data_Warehouse/datasets"  # where source_crm/ and source_erp/ live


def get_engine():
    url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return create_engine(url)


def load_csv_to_postgres(engine):
    for folder, schema in SOURCE_MAP.items():
        folder_path = os.path.join(DATA_DIR, folder)
        if not os.path.exists(folder_path):
            print(f"⚠️ Folder {folder_path} not found, skipping.")
            continue

        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                file_path = os.path.join(folder_path, file)
                table_name = os.path.splitext(file)[0].lower()

                print(f"📦 Loading {file} → {schema}.{table_name}")

                df = pd.read_csv(file_path)
                df.columns = [c.strip().replace(" ", "_").replace("-", "_").lower() for c in df.columns]

                df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)
                print(f"✅ Loaded {len(df)} rows into {schema}.{table_name}\n")


if __name__ == "__main__":
    engine = get_engine()
    load_csv_to_postgres(engine)
    print("🎉 All CSVs loaded successfully!")
