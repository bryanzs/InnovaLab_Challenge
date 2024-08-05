from src.etl import ETL
from src.make_dir import create_directory_tree


if __name__=='__main__':
    create_directory_tree()

    etl = ETL()
    etl.extract()
    etl.transform()
    etl.load()
