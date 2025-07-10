require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const csv = require('fast-csv');

const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT
});

const parseDate = (str) => {
  const date = new Date(str);
  return isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
};

async function importCompanies() {
  try {
    const client = await pool.connect();
    console.log('Connected to PostgreSQL');

    const stream = fs.createReadStream(process.env.CSV_PATH)
      .pipe(csv.parse({ headers: true, renameHeaders: true, trim: true, maxRows: 1000 }))
      .on('data', async (row) => {
        stream.pause();

        if (!row['CompanyNumber']) {
          console.warn('Skipping bad row:', row);
          stream.resume();
          return;
        }

        const sicArray = [
          row['SICCode.SicText_1'],
          row['SICCode.SicText_2'],
          row['SICCode.SicText_3'],
          row['SICCode.SicText_4']
        ].filter(Boolean);

        const query = `
          INSERT INTO companies (
            company_number, name, address_line_1, address_line_2, post_town,
            county, country, postcode, category, status,
            incorporation_date, dissolution_date, sic_codes,
            accounts_ref_day, accounts_ref_month, accounts_next_due_date,
            accounts_last_made_up_date, accounts_category
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
          ON CONFLICT (company_number) DO NOTHING;
        `;

        try {
          await client.query(query, [
            row['CompanyNumber'] || null,
            row['CompanyName'] || null,
            row['RegAddress.AddressLine1'] || null,
            row['RegAddress.AddressLine2'] || null,
            row['RegAddress.PostTown'] || null,
            row['RegAddress.County'] || null,
            row['RegAddress.Country'] || null,
            row['RegAddress.PostCode'] || null,
            row['CompanyCategory'] || null,
            row['CompanyStatus'] || null,
            parseDate(row['IncorporationDate']),
            parseDate(row['DissolutionDate']),
            sicArray.length ? sicArray : null,
            row['Accounts.AccountRefDay'] ? parseInt(row['Accounts.AccountRefDay']) : null,
            row['Accounts.AccountRefMonth'] ? parseInt(row['Accounts.AccountRefMonth']) : null,
            parseDate(row['Accounts.NextDueDate']),
            parseDate(row['Accounts.LastMadeUpDate']),
            row['Accounts.AccountCategory'] || null
          ]);
        } catch (err) {
          console.error(`Error inserting ${row['CompanyNumber']}: ${err.message}`);
        }

        stream.resume();
      })
      .on('error', (err) => console.error('CSV Error:', err.message))
      .on('end', async () => {
        console.log('Import complete');
        await client.release();
        await pool.end();
      });
  } catch (err) {
    console.error('Database Error:', err.message);
  }
}

importCompanies();
