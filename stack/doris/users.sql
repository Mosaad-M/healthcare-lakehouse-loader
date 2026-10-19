-- Doris users: built-in users here; LDAP or SSO in production.
-- Applied by doris-init. ${...} values come from .env. Passwords are reset on every apply.
CREATE ROLE IF NOT EXISTS analyst;
CREATE ROLE IF NOT EXISTS care_team;
CREATE USER IF NOT EXISTS 'analyst'@'%' IDENTIFIED BY '${DORIS_ANALYST_PASSWORD}' DEFAULT ROLE 'analyst';
CREATE USER IF NOT EXISTS 'analyst_ma'@'%' IDENTIFIED BY '${DORIS_ANALYST_MA_PASSWORD}' DEFAULT ROLE 'analyst';
CREATE USER IF NOT EXISTS 'care_team'@'%' IDENTIFIED BY '${DORIS_CARE_TEAM_PASSWORD}' DEFAULT ROLE 'care_team';
SET PASSWORD FOR 'analyst'@'%' = PASSWORD('${DORIS_ANALYST_PASSWORD}');
SET PASSWORD FOR 'analyst_ma'@'%' = PASSWORD('${DORIS_ANALYST_MA_PASSWORD}');
SET PASSWORD FOR 'care_team'@'%' = PASSWORD('${DORIS_CARE_TEAM_PASSWORD}');
-- The serving refresh (flow runs) as its own user, never root: it rebuilds the copies in the serving
-- database and re-grants them, and reads Gold through the lake catalogs, nothing else.
CREATE ROLE IF NOT EXISTS refresher;
CREATE USER IF NOT EXISTS 'serving_refresher'@'%' IDENTIFIED BY '${DORIS_REFRESHER_PASSWORD}' DEFAULT ROLE 'refresher';
SET PASSWORD FOR 'serving_refresher'@'%' = PASSWORD('${DORIS_REFRESHER_PASSWORD}');
GRANT SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV, GRANT_PRIV ON internal.serving.* TO ROLE 'refresher';
GRANT SELECT_PRIV ON lake_phi.gold.* TO ROLE 'refresher';
GRANT SELECT_PRIV ON lake_deid.gold.* TO ROLE 'refresher';
