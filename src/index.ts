import pool from './config';

async function getFirstRecords(): Promise<any[]> {
    try {
        const res = await pool.query('SELECT * FROM brreg_enheter_alle LIMIT 10');
        return res.rows;
    } catch (err) {
        console.log(err);
        throw err;
    }
}

function displayRecords(records: any[]) {
    console.log("\nDisplaying records:");
    console.table(records.map(record => ({
        "Organisasjonsnummer": record.organisasjonsnummer,
        "Navn": record.navn,
        "Hjemmeside": record.hjemmeside,
        "Antall ansatte": record.antall_ansatte
    })));
}

async function getLargestOrganizations(numberOfOrganizations: number) {
    let client = await connect();
    try {
        let results = await client.query(`SELECT organisasjonsnummer, navn, hjemmeside, antall_ansatte FROM brreg_enheter_alle ORDER BY antall_ansatte DESC LIMIT $1`, [numberOfOrganizations]);
        displayRecords(results.rows);
        return results.rows;
    } finally {
        client.release();
    }
}



async function main() {
    let records = await getFirstRecords();
    displayRecords(records);
    let largestOrganizations = await getLargestOrganizations(5);

}
console.log("Starting")
main();