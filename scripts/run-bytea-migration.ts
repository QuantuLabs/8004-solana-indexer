import pg from 'pg';

// Try direct connection (not pooler)
const dsn = "postgresql://postgres:XnxYuHI8ZdpnDyd1@db.uhjytdjxvfbppgjicfly.supabase.co:5432/postgres";

async function runMigration() {
  const client = new pg.Client({ connectionString: dsn });
  
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
