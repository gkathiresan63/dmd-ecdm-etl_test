"""Snapshot Configuration definition for EnterpriseCustomerDataManagement project."""

from ecdm_etl.snapshot.snapshot_table import SnapshotTable

SNAPSHOT_TABLES = []

# Add ecdm_c_b_addr_out for dim_address snapshot
ecdm_c_b_addr_out_merge_columns = [
    "ADDR_LINE_1",
    "ADDR_LINE_2",
    "ADDR_LINE_3",
    "ADDR_LINE_4",
    "CITY",
    "STATE",
    "POSTAL_CD",
    "POSTAL_EXT_CD",
    "ZIP_10_13_NR",
    "COUNTRY_CD",
    "ADDR_HASH_KEY",  # ADDR_HASH_KEY is not in the natural key calculation but we need it for t
]

SNAPSHOT_TABLES.append(
    SnapshotTable("ecdm_c_b_addr_out", "ecdm_c_b_addr_out_snapshot", ecdm_c_b_addr_out_merge_columns)
)

# Add ecdm_c_b_bene_dsgn_out_snapshot for dim_agreement snapshot
ecdm_c_b_bene_dsgn_out_merge_columns = ["CARR_ADMN_SYS_CD", "HLDG_KEY_PFX", "AGRMNT_NUM", "HLDG_KEY_SFX"]

SNAPSHOT_TABLES.append(
    SnapshotTable("ecdm_c_b_bene_dsgn_out", "ecdm_c_b_bene_dsgn_out_snapshot", ecdm_c_b_bene_dsgn_out_merge_columns)
)

# Add ecdm_prty_agmt_addr_out_snapshot for rel_party_address snapshot
ecdm_prty_agmt_addr_out_merge_columns = ["MEMBER_ID", "HLDG_KEY_PFX", "AGRMNT_NUM", "HLDG_KEY_SFX"]

SNAPSHOT_TABLES.append(
    SnapshotTable("ecdm_party_agmt_addr_out", "ecdm_prty_agmt_addr_out_snapshot", ecdm_prty_agmt_addr_out_merge_columns)
)
