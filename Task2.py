import os
import mysql.connector
import pandas as pd
import pytest
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


def get_query(query):
    """
    Executes an SQL query and returns the result as a pandas data frame.

    Args:
        query (str): SQL query to execute.

    Returns:
        pandas.DataFrame: The result of the SQL query as a pandas data frame.

    Raises:
        mysql.connector.Error: If there's an error executing the query.

    Example:
        >>> df = get_query("SELECT * FROM patients")
    """
    try:
        cnx = mysql.connector.connect(
            host=os.environ.get('DB_HOST'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME')
        )
        cursor = cnx.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        cnx.close()
        return df
    except mysql.connector.Error as e:
        raise e


def get_person(patient_ids):
    """
    Retrieves patient data from database based on provided patient IDs.

    Args:
        patient_ids (list[int]): List of patient IDs to retrieve.

    Returns:
        pandas.DataFrame: The retrieved patient data as a pandas data frame.
    """
    db_host = os.environ.get('DB_HOST')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_name = os.environ.get('DB_NAME')

    cnx = mysql.connector.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        database=db_name
    )
    cursor = cnx.cursor()

    patient_ids_str = ', '.join(map(str, patient_ids))
    query = f"SELECT * FROM patients WHERE patient_id IN ({patient_ids_str})"

    cursor.execute(query)
    result = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df = pd.DataFrame(result, columns=columns)

    cursor.close()
    cnx.close()

    return df


def test_get_query():
    # Test empty query
    assert get_query("") is None

    # Test invalid query
    with pytest.raises(mysql.connector.Error):
        get_query("SELECT * FROM nonexistent_table")

    # Test valid query
    df = get_query("SELECT * FROM patients")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert set(df.columns) == set(['patient_id', 'birth_date', 'gender', 'race', 'ethnicity'])
