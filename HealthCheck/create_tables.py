import os

from sqlalchemy import create_engine
import yaml

from models import Base


with open("/config/health_check_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())


DATASTORE_FILE = app_config["datastore"]["filename"]
os.makedirs(os.path.dirname(DATASTORE_FILE), exist_ok=True)


def main():
    engine = create_engine(
        f"sqlite:///{DATASTORE_FILE}",
        connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    print(f"Tables created (or already exist) in {DATASTORE_FILE}")


if __name__ == "__main__":
    main()
