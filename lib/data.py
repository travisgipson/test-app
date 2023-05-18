import sqlalchemy as sqla

def create_psql_eng(dbcreds):
    
    # creates postgres connection string
    sql_str = "postgresql+psycopg2://{usr}:{pw}@{host}:{port}/{db}".format(usr=dbcreds["usr"],
                                                                            pw=dbcreds["pw"],
                                                                            host=dbcreds["host"],
                                                                            port=dbcreds["port"],
                                                                            db=dbcreds["db"])
    return sqla.create_engine(sql_str).connect()
