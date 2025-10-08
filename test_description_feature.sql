-- Test file to demonstrate the new description feature
-- This would be used with a DataFusion session to test the functionality

-- First, let's show columns without EXTENDED/FULL (should not include description)
SHOW COLUMNS FROM some_table;

-- Now with EXTENDED (should include description column)
SHOW COLUMNS FROM some_table EXTENDED;

-- And with FULL (should also include description column)
SHOW COLUMNS FROM some_table FULL;

-- Direct query to information_schema.columns to see all columns including description
SELECT * FROM information_schema.columns WHERE table_name = 'some_table';