"""CLI entrypoint for ecdm_etl.

Usage:
    ecdm_etl --help
    ecdm_etl [options]

Options:
  -h --help                         Show this screen
  --db-host=HOST                    EDW Vertca hostname
  --db-port=PORT                    EDW Vertica port number.
  --db-name=DB_NAME                 EDW Database name.
  --db-user=DB_USER                 Project batch user name.
  --db-password=DB_PASSWORD         Project batch user password.
  --etl-step=ETL_STEP               ETL step: all, staging, pre_transform_check, transform, merge, post_steps.
  --audit-schema=AUDIT_SCHEMA       Vertica schema name of edw audit tables. [default: edw_audit].
  --mdc-schema=MDC_SCHEMA           Metadata catalog Vertica schema
  --job-scheduler=JOB_SCHEDULER     Job scheduler: jenkins, manual, airflow, etc.
  --support-email-address=EMAIL     Supporter email address.
  --debug                           If set, log_level = DEBUG
  --trans-dt=TRANS_DT               Need to be passed when process old batch to indicate source date.[default:].
  --dry-run                         Dry run the ETL project,will NOT do any update on DB.
  --initial-load                    Run through initial load logic, [default: False]
"""


import logging

from docopt import docopt

from ecdm_etl.ecdm_etl import EnterpriseCustomerDataManagementETL
from ecdm_etl.initial_load import EnterpriseCustomerDataManagementInitialLoad


def main():
    """Enterprise Customer Data Management ETL application Entrypoint."""
    args = docopt(__doc__)
    log_level = logging.INFO if args["--log-level"].upper() == "INFO" else logging.DEBUG
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=log_level)
    debug_mode = log_level is logging.DEBUG
    if not args["--initial-load"]:
        ecdm = EnterpriseCustomerDataManagementETL(
            db_host=args["--db-host"],
            db_port=args["--db-port"],
            db_name=args["--db-name"],
            db_user=args["--db-user"],
            db_password=args["--db-password"],
            etl_step=args["--etl-step"],
            mdc_schema=args["--mdc-schema"],
            audit_schema=args["--audit-schema"],
            job_scheduler=args["--job-scheduler"],
            support_email_address=args["--support-email-address"],
            trans_dt=args["--trans-dt"],
            debug_mode=debug_mode,
            dry_run=args["--dry-run"],
        )
    else:
        ecdm = EnterpriseCustomerDataManagementInitialLoad(
            db_host=args["--db-host"],
            db_port=args["--db-port"],
            db_name=args["--db-name"],
            db_user=args["--db-user"],
            db_password=args["--db-password"],
            debug_mode=debug_mode,
        )

    ecdm.run()


if __name__ == "__main__":
    main()
