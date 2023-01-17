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


async function main() {
    let records = await getFirstRecords();
    displayRecords(records);
}
console.log("Starting")
main();