-- What each role may read in Doris. Applied by the refresher after every serving refresh (a
-- recreated copy loses its grants). Nobody but root and the refresher (Gold only) reads the lake
-- catalogs (lake_phi, lake_deid) directly. Row policies are in policies.sql (set up by root).
-- Analysts: the de-identified copies only.
GRANT SELECT_PRIV ON internal.serving.claims_monthly TO ROLE 'analyst';
GRANT SELECT_PRIV ON internal.serving.provider_activity TO ROLE 'analyst';
GRANT SELECT_PRIV ON internal.serving.lab_monthly TO ROLE 'analyst';
-- Care team: the member mart, without the (encrypted) identifier column.
GRANT SELECT_PRIV(subject_id, claims, first_service_date, last_service_date, billed_amount, paid_amount)
    ON internal.serving.member_claims TO ROLE 'care_team';
