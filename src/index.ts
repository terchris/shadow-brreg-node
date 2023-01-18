import pool from './config';


function displayRecords(records: any[]) {
    console.log("\nDisplaying records:");
    console.table(records.map(record => ({
        "Organisasjonsnummer": record.organisasjonsnummer,
        "Navn": record.navn,
        "Hjemmeside": record.hjemmeside,
        "Antall ansatte": record.antall_ansatte,
        "count": record.count,
        "duplicate_hjemmeside": record.duplicate_hjemmeside
    })));
}

async function getOrganizations(query: string, limit: number) : Promise<any[]> {
    try {
        const res = await pool.query(`${query} LIMIT $1`, [limit]);
        return res.rows;
    } catch (err) {
        console.log(err);
        throw err;
    }
}

async function addFieldToTable(tableName: string, fieldName: string, fieldType: string) : Promise<boolean> {
    try {
        const res = await pool.query(`SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`, [tableName, fieldName]);
        if (res.rows.length === 0) {
            await pool.query(`ALTER TABLE ${tableName} ADD COLUMN ${fieldName} ${fieldType}`);
        }
        return true;
    } catch (err) {
        console.log(err);
        return false;
    }
}



async function main() {

    const numberOfOrganizations = 10;
    const firstOrganizationsQuery = `SELECT * FROM brreg_enheter_alle`;
    const largestOrganizationsQuery = `SELECT organisasjonsnummer, navn, hjemmeside, antall_ansatte FROM brreg_enheter_alle ORDER BY antall_ansatte DESC`;
    const largestOrganizationsNoWebQuery = `SELECT organisasjonsnummer, navn, hjemmeside, antall_ansatte FROM brreg_enheter_alle WHERE (hjemmeside IS NULL OR hjemmeside = '') ORDER BY antall_ansatte DESC`;
    const duplicateWebQuery = `SELECT hjemmeside, COUNT(*) as count FROM brreg_enheter_alle WHERE hjemmeside IS NOT NULL AND hjemmeside != '' GROUP BY hjemmeside ORDER BY count DESC`;
    const duplicateWebCountQuery = `SELECT COUNT(hjemmeside) as count, hjemmeside FROM brreg_enheter_alle GROUP BY hjemmeside HAVING COUNT(hjemmeside) > 1 ORDER BY count DESC`;
    const duplicateWebTableQuery = `SELECT COUNT(*) as count, duplicate_hjemmeside FROM (
        SELECT COUNT(hjemmeside) as duplicate_hjemmeside 
        FROM brreg_enheter_alle 
        GROUP BY hjemmeside
    ) subquery 
    GROUP BY duplicate_hjemmeside
    ORDER BY duplicate_hjemmeside DESC`;


    const addFiled_sync_date = `ALTER TABLE brreg_enheter_alle ADD COLUMN sync_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP`;

    console.log("Getting first records in the table");
    let firstRecords = await getOrganizations(firstOrganizationsQuery, numberOfOrganizations);
    displayRecords(firstRecords);

    console.log("Getting largest organizations");
    const largestOrganizations = await getOrganizations(largestOrganizationsQuery, numberOfOrganizations);
    displayRecords(largestOrganizations);

    console.log("Getting largest organizations with no website");
    const largestOrganizationsNoWeb = await getOrganizations(largestOrganizationsNoWebQuery, numberOfOrganizations);
    displayRecords(largestOrganizationsNoWeb);

    console.log("Getting duplicate websites");
    const duplicateWeb = await getOrganizations(duplicateWebQuery, numberOfOrganizations);
    displayRecords(duplicateWeb);

    console.log("Getting duplicate websites count");
    const duplicateWebCount = await getOrganizations(duplicateWebCountQuery, numberOfOrganizations);
    displayRecords(duplicateWebCount);

    console.log("Getting duplicate websites table");
    const duplicateWebTable = await getOrganizations(duplicateWebTableQuery, 10000);
    displayRecords(duplicateWebTable);


    console.log("Adding sync_date field");
    let sucessAdding = await addFieldToTable('brreg_enheter_alle', 'urb_sync_date', 'TIMESTAMP');



}

console.log("Starting")
main();