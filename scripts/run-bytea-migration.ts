import { Client } from 'pg';

// Prefer a direct DB connection (not the pooler) for migrations.
const dsn = process.env.SUPABASE_DSN_DIRECT || process.env.SUPABASE_DSN;
if (!dsn) {
  console.error(
    'Missing SUPABASE_DSN (or SUPABASE_DSN_DIRECT). Example: export SUPABASE_DSN="POSTGRES_DSN_REDACTED"'
  );
  process.exit(1);
}

const sslVerify = process.env.SUPABASE_SSL_VERIFY !== 'false';
const ssl = { rejectUnauthorized: sslVerify };

async function runMigration() {
  const client = new Client({ connectionString: dsn, ssl });
  
  try {
    await client.connect();
    console.log("Connected to Supabase");
    
    // Check current column type
    const checkResult = await client.query(`
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = 'metadata' AND column_name = 'value';
    `);
    console.log("Current column type:", checkResult.rows);
    
    if (checkResult.rows[0]?.data_type === 'bytea') {
      console.log("Column already BYTEA, skipping migration");
      return;
    }
    
    // Run migration
    console.log("Running BYTEA migration...");
    await client.query(`
      ALTER TABLE metadata
      ALTER COLUMN value TYPE BYTEA
      USING decode(value, 'base64');
    `);
    
    // Add comment
    await client.query(`
      COMMENT ON COLUMN metadata.value IS 'Binary metadata value (raw bytes, previously base64 TEXT)';
    `);
    
    console.log("Migration completed!");
    
    // Verify
    const verifyResult = await client.query(`
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = 'metadata' AND column_name = 'value';
    `);
    console.log("New column type:", verifyResult.rows);
    
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await client.end();
  }
}

runMigration();
