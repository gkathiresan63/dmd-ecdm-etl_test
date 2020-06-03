"""Initial Load."""

import logging

import vertica_python

from generic_etl.vertica_utils import VerticaUtils


class EnterpriseCustomerDataManagementInitialLoad:
    """EnterpriseCustomerDataManagement initial load application."""

    def __init__(
        self, db_host: str, db_port: int, db_name: str, db_user: str, db_password: str, debug_mode: bool = False
    ) -> None:
        """Generate a EnterpriseCustomerDataManagementETL object.

        :param db_host: edw server hostname.
        :param db_port: edw server port number.
        :param db_name: edw database name.
        :param db_user: edw batch user for this project.
        :param db_password: password for the edw batch user.
        :param debug_mode: split bulk transform statements(in SQL file) to run individually for easier troubleshooting.
        """
        self.conn_info = {
            "host": db_host,
            "port": db_port,
            "user": db_user,
            "password": db_password,
            "database": db_name,
            # autogenerated session label by default,
            "session_label": "some_label",
            # default throw error on invalid UTF-8 results
            "unicode_error": "strict",
            # SSL is disabled by default
            "ssl": True,
            # using server-side  prepared statements is disabled by default
            "use_prepared_statements": False,
        }

        self.logger = logging.getLogger(self.__class__.__name__)
        self.debug_mode = debug_mode

    def get_connection(self):
        """Create a Vertica connection from the given connection information."""
        return vertica_python.connect(**self.conn_info)

    def run(self):
        """Start executing initial load logic."""
        root_path_prefix = "sql/initial_load/edw/"

        # Some of the initial load sql will only need to be execute once."""
        sql_files = (
            "dim_address/ecdm_c_b_addr_out_snapshot.sql",
            "dim_address/ecdm_dim_address.sql",
            "dim_agreement/ecdm_c_b_bene_dsgn_out_snapshot.sql",
            "dim_agreement/ecdm_dim_agreement.sql",
            "rel_party_address/ecdm_rel_party_address.sql",
        )
        ecdm_initial_load_tables = (f"{root_path_prefix}{s}" for s in sql_files)

        with self.get_connection() as conn:
            for ecdm_initial_table in ecdm_initial_load_tables:
                self.logger.info(f"Loading {ecdm_initial_table}")
                VerticaUtils.run_query_from_sql_file(conn, ecdm_initial_table, {}, with_commit=True)
