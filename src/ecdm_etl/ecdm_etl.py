"""ETL implementation for EnterpriseCustomerDataManagement project."""
from typing import Any
from typing import Dict
from typing import cast

from generic_etl.generic_etl import GenericETL
from generic_etl.mdc.model import ETLStep
from generic_etl.vertica_utils import VerticaUtils

from ecdm_etl.snapshot.snapshot_config import SNAPSHOT_TABLES

ETL_PROJECT_NAME = "Enterprise_Customer_Data_Management"


class EnterpriseCustomerDataManagementETL(GenericETL):
    """EnterpriseCustomerDataManagement ETL application, inherit common functionalities from GenericETL class."""

    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
        mdc_schema: str,
        audit_schema: str,
        etl_step: str,
        job_scheduler: str,
        support_email_address: str,
        trans_dt: str,
        debug_mode: bool = False,
        dry_run: bool = False,
    ) -> None:
        """Generate a EnterpriseCustomerDataManagementETL object.

        :param db_host: edw server hostname.
        :param db_port: edw server port number.
        :param db_name: edw database name.
        :param db_user: edw batch user for this project.
        :param db_password: password for the edw batch user.
        :param mdc_schema: schema name for the metadata catalog.
        :param job_scheduler: job scheduler name (Jenkins, Airflow, etc.
        :param support_email_address: project supporter email address.
        :param debug_mode: If True, log_level = DEBUG
        :param dry_run: Dry run the ETL project, will NOT do any update on DB.
        """
        super().__init__(
            db_host,
            db_port,
            db_name,
            db_user,
            db_password,
            mdc_schema,
            audit_schema,
            ETL_PROJECT_NAME,
            etl_step,
            job_scheduler,
            support_email_address,
            trans_dt=trans_dt,
            debug_mode=debug_mode,
            dry_run=dry_run,
        )
        self._etl_param_values = cast(Dict[str, Any], {})
        self.STAGING_SCHEMA = "edw_staging"
        self.snapshot_tables = SNAPSHOT_TABLES

    def run(self):
        """Run each of the ETL steps."""
        """Run snapshot before Transformation"""
        with self.get_connection() as conn:
            self._run_snapshot_for_datasets(conn, self.snapshot_tables)

            self._create_new_batch_and_step_audit(conn)

            steps = [ETLStep.TRANSFORM, ETLStep.MERGE]
            for step in steps:
                self.etl_step = step
                step_runner = self.step_lambda_map[self.etl_step]
                self.skipped_steps = []
                self.skipped_tables = []
                step_runner()

    def _run_snapshot_for_datasets(self, conn, snapshot_tables):
        for snapshot_table in snapshot_tables:
            VerticaUtils.merge_to_snapshot_table(
                conn,
                self.STAGING_SCHEMA,
                snapshot_table.source_table_name,
                self.STAGING_SCHEMA,
                snapshot_table.snapshot_table_name,
                snapshot_table.snapshot_merge_columns,
            )
