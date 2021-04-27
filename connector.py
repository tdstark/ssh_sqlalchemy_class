import os
import codecs
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from sshtunnel import SSHTunnelForwarder


class DatabaseConn:
    """
    This class returns a SSH connection to postgres. A
    Database variable must be called in the with statement.
    Exception handling should be outside of this class.
    Note: WILL ONLY WORK WITH PANDAS IF CONN.EXECUTE
    OR TO_SQL WITH METHOD='MULTI' IS CALLED.
    """
    def __init__(self, database):
        self.database = database
        self.engine = ""
        self.server = SSHTunnelForwarder(str(os.getenv('POSTGRES_HOST')),
                                         ssh_port=22,
                                         ssh_username=codecs.decode(str(os.getenv('SSH_USER')), 'rot-13'),
                                         ssh_pkey="~/.ssh/id_rsa",
                                         remote_bind_address=('localhost', 5432))

    def __enter__(self):
        logger.info("Attempting to establish SSH connection.")

        self.server.start()

        # Database connection variables
        user = codecs.decode(str(os.getenv('POSTGRES_USER')), 'rot-13')
        password = codecs.decode(str(os.getenv('POSTGRES_PASSWORD')), 'rot-13')
        local_port = str(self.server.local_bind_port)
        conn_string = f"postgresql://{user}:{password}@127.0.0.1:{local_port}/{self.database}"

        # Database engine
        self.engine = create_engine(conn_string,
                                    poolclass=StaticPool,
                                    echo=True,
                                    echo_pool=True)
        logger.info("SSH connection established.")
        return self.engine

    def __exit__(self, exc_type, exc, exc_tb):
        logger.info("Attempting to close SSH connection.")
        self.engine.dispose()
        self.server.stop(force=True)
        logger.info("SSH connection closed.")