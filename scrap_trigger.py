import time 
import psycopg2

db_params = {
    'dbname': 'reliance',
    'user': 'docker',
    'password': 'docker',
    'host': "192.168.1.188",#'192.168.1.223',#"192.168.29.101",#
    'port': 5432
}

conn = psycopg2.connect(**db_params)

cur = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS profit_loss_data (
    index BIGINT primary key,
    Year TEXT,
    Sales BIGINT,
    Expenses BIGINT,
    Operating_Profit BIGINT,
    OPM_Percent INTEGER,
    Other_Income BIGINT,
    Interest BIGINT,
    Depreciation BIGINT,
    Profit_before_tax BIGINT,
    Tax_Percent INTEGER,
    Net_Profit BIGINT,
    EPS_in_Rs DOUBLE PRECISION,
    Dividend_Payout_Percent INTEGER,
    Stock TEXT
);


CREATE TABLE IF NOT EXISTS profit_loss_changes (
    change_id SERIAL PRIMARY KEY,
    operation TEXT,
    index BIGINT,
    Year TEXT,
    Sales BIGINT,
    Expenses BIGINT,
    Operating_Profit BIGINT,
    OPM_Percent INTEGER,
    Other_Income BIGINT,
    Interest BIGINT,
    Depreciation BIGINT,
    Profit_before_tax BIGINT,
    Tax_Percent INTEGER,
    Net_Profit BIGINT,
    EPS_in_Rs DOUBLE PRECISION,
    Dividend_Payout_Percent INTEGER,
    Stock TEXT
);


CREATE OR REPLACE FUNCTION log_profit_loss_changes() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO profit_loss_changes (
            operation, index, Year, Sales, Expenses, Operating_Profit, OPM_Percent,
            Other_Income, Interest, Depreciation, Profit_before_tax, Tax_Percent, Net_Profit,
            EPS_in_Rs, Dividend_Payout_Percent,Stock
        ) VALUES (
            'INSERT', NEW.index, NEW.Year, NEW.Sales, NEW.Expenses, NEW.Operating_Profit, NEW.OPM_Percent,
            NEW.Other_Income, NEW.Interest, NEW.Depreciation, NEW.Profit_before_tax, NEW.Tax_Percent,
            NEW.Net_Profit, NEW.EPS_in_Rs,NEW.Dividend_Payout_Percent, NEW.Stock
        );
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO profit_loss_changes (
            operation, index, Year, Sales, Expenses, Operating_Profit, OPM_Percent,
            Other_Income, Interest, Depreciation, Profit_before_tax, Tax_Percent, Net_Profit,
            EPS_in_Rs, Dividend_Payout_Percent,Stock
        ) VALUES (
            'UPDATE', NEW.index, NEW.Year, NEW.Sales, NEW.Expenses, NEW.Operating_Profit, NEW.OPM_Percent,
            NEW.Other_Income, NEW.Interest, NEW.Depreciation, NEW.Profit_before_tax, NEW.Tax_Percent,
            NEW.Net_Profit, NEW.EPS_in_Rs,NEW.Dividend_Payout_Percent, NEW.Stock
        );
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO profit_loss_changes (
            operation, index, Year, Sales, Expenses, Operating_Profit, OPM_Percent,
            Other_Income, Interest, Depreciation, Profit_before_tax, Tax_Percent, Net_Profit,
            EPS_in_Rs, Dividend_Payout_Percent,Stock
        ) VALUES (
            'DELETE', OLD.index, OLD.Year, OLD.Sales, OLD.Expenses, OLD.Operating_Profit, OLD.OPM_Percent,
            OLD.Other_Income, OLD.Interest, OLD.Depreciation, OLD.Profit_before_tax, OLD.Tax_Percent,
            OLD.Net_Profit, OLD.EPS_in_Rs,OLD.Dividend_Payout_Percent, OLD.Stock
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS profit_loss_changes_trigger ON profit_loss_data;

CREATE TRIGGER profit_loss_changes_trigger
AFTER INSERT OR UPDATE OR DELETE ON profit_loss_data
FOR EACH ROW EXECUTE FUNCTION log_profit_loss_changes();
'''

cur.execute(create_table_query)
conn.commit()
print("Table and trigger table created")



insert_query = '''
INSERT INTO profit_loss_data (
    Index,Year, Sales, Expenses, Operating_Profit, OPM_Percent, Other_Income, Interest, Depreciation,
    Profit_before_tax, Tax_Percent, Net_Profit, EPS_in_Rs,Dividend_Payout_Percent, Stock
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s);
'''

for x, row in df_table.iterrows():
    cur.execute(insert_query, tuple(row))
    conn.commit() 
    print(f"Inserted data for year: {row['index']}")
    time.sleep(3)
