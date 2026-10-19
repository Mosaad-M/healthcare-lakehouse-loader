-- Row policies, applied by root at setup (creating one needs a global privilege). A policy is bound
-- to the table's name, so it survives the refresher dropping and recreating a copy.
-- analyst_ma: provider activity in Massachusetts only.
CREATE ROW POLICY IF NOT EXISTS provider_activity_ma ON internal.serving.provider_activity
    AS RESTRICTIVE TO 'analyst_ma' USING (state = 'MA');
