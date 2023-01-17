import pool from './config';


function displayRecords(records: any[]) {
    console.log("\nDisplaying records:");
    console.table(records.map(record => ({
        "Organisasjonsnummer": record.organisasjonsnummer,
        "Navn": record.navn,
        "Hjemmeside": record.hjemmeside,
        "Antall ansatte": record.antall_ansatte,
        "count": record.count
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


async function main() {

    const numberOfOrganizations = 10;
    const firstOrganizationsQuery = `SELECT * FROM brreg_enheter_alle`;
    const largestOrganizationsQuery = `SELECT organisasjonsnummer, navn, hjemmeside, antall_ansatte FROM brreg_enheter_alle ORDER BY antall_ansatte DESC`;
    const largestOrganizationsNoWebQuery = `SELECT organisasjonsnummer, navn, hjemmeside, antall_ansatte FROM brreg_enheter_alle WHERE (hjemmeside IS NULL OR hjemmeside = '') ORDER BY antall_ansatte DESC`;
    const duplicateWebQuery = `SELECT hjemmeside, COUNT(*) as count FROM brreg_enheter_alle WHERE hjemmeside IS NOT NULL AND hjemmeside != '' GROUP BY hjemmeside ORDER BY count DESC`;


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

}

console.log("Starting")
main();