import pool from './config.js';
import pgFormat from "pg-format";
import axios from 'axios';

import { IBrregEnheterAlle, IOppdaterteEnheter } from './typedefinitions';

let globalOppdaterteEnheter = 0;

async function initiateUrbalurbaStatus() {
    try {
        // Check if the table urbalurba_status exists
        const res = await pool.query(`SELECT to_regclass('urbalurba_status')`);
        const tableExists = res.rows[0].to_regclass;

        if (!tableExists) {
            // Create the table urbalurba_status
            await pool.query(`CREATE TABLE urbalurba_status (
                database_download_date TIMESTAMP,
                last_brreg_update_date TIMESTAMP,
                last_brreg_oppdateringsid INTEGER,
                id Integer 
            )`);

            // Get the date for when the database was created
            const dbCreateDate = await pool.query(`SELECT (pg_stat_file('base/'||oid ||'/PG_VERSION')).modification
            FROM pg_database
            WHERE datname = 'importdata'`);

            // Change the date to a second past midnight
            const database_download_date = new Date(dbCreateDate.rows[0].modification);
            database_download_date.setUTCHours(0,0,1);

            // Insert the date into the urbalurba_status table
            await pool.query(`INSERT INTO urbalurba_status (database_download_date, id) VALUES ($1, 1)`, [database_download_date]);
            console.log("urbalurba_status table is ready and initialized with the date of when the database was created. Witch is: " + database_download_date);
        } else {
            console.log("urbalurba_status table already exists");
        }

    } catch (err) {
        console.log(err);
        throw err;
    }
}

async function initiateOppdaterteEnheter(): Promise<boolean> {
    try {
        await pool.query(`CREATE TABLE IF NOT EXISTS oppdaterteEnheter (oppdateringsid INTEGER PRIMARY KEY, dato TIMESTAMP, organisasjonsnummer VARCHAR(255), endringstype VARCHAR(255), urb_processed TIMESTAMP);`);
        await pool.query(`ALTER TABLE oppdaterteEnheter ADD COLUMN IF NOT EXISTS urb_processed TIMESTAMP;`);
        return true;
    } catch (err) {
        console.log(err);
        return false;
    }
}



async function initiateBrregEnheterAlle() {
    try {
        // Check if the fields already exist
        const res = await pool.query(`
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'brreg_enheter_alle'
            AND column_name IN ('urb_brreg_oppdateringsid', 'urb_brreg_update_date', 'urb_brreg_endringstype', 'urb_sync_date');
        `);

        // If the fields don't exist, add them
        if (res.rows.length === 0) {
            await pool.query(`
                ALTER TABLE brreg_enheter_alle
                ADD COLUMN urb_brreg_oppdateringsid INTEGER,
                ADD COLUMN urb_brreg_update_date TIMESTAMP,
                ADD COLUMN urb_brreg_endringstype VARCHAR(255),
                ADD COLUMN urb_sync_date TIMESTAMP;
            `);
            console.log("Fields added successfully to brreg_enheter_alle table");
        } else {
            console.log("Fields already exist in brreg_enheter_alle table");
        }
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
        "Antall ansatte": record.antall_ansatte,
        "count": record.count,
        "duplicate_hjemmeside": record.duplicate_hjemmeside
    })));
}

async function getOrganizations(query: string, limit: number): Promise<any[]> {
    try {
        const res = await pool.query(`${query} LIMIT $1`, [limit]);
        return res.rows;
    } catch (err) {
        console.log(err);
        throw err;
    }
}

async function addFieldToTable(tableName: string, fieldName: string, fieldType: string): Promise<boolean> {
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



async function addOppdaterteEnheter(inputJson: any): Promise<boolean> {
    try {
        const oppdaterteEnheter = inputJson._embedded.oppdaterteEnheter;
        for (const enhet of oppdaterteEnheter) {
            const oppdateringsid = enhet.oppdateringsid;
            // Check if the oppdateringsid already exists in the table
            const res = await pool.query("SELECT oppdateringsid FROM oppdaterteEnheter WHERE oppdateringsid = $1", [oppdateringsid]);
            if (res.rows.length === 0) {
                // oppdateringsid does not exist, insert the record
                const dato = enhet.dato;
                const organisasjonsnummer = enhet.organisasjonsnummer;
                const endringstype = enhet.endringstype;
                await pool.query("INSERT INTO oppdaterteEnheter (oppdateringsid, dato, organisasjonsnummer, endringstype) VALUES ($1, $2, $3, $4)", [oppdateringsid, dato, organisasjonsnummer, endringstype]);
            }
        }
        return true;
    } catch (err) {
        console.log(err);
        return false;
    }
}



async function getPreviousDate(): Promise<string> {
    try {
        const res = await pool.query(`SELECT database_download_date, last_brreg_update_date FROM urbalurba_status LIMIT 1`);
        const date = res.rows[0].last_brreg_update_date || res.rows[0].database_download_date;
        return date.toISOString();
    } catch (err) {
        console.log(err);
        throw err;
    }
}

async function updatePreviousDate(lastUpdate: { oppdateringsid: number, dato: string }) {
    try {
        await pool.query(
            `UPDATE urbalurba_status SET last_brreg_update_date = $1, last_brreg_oppdateringsid = $2 WHERE id = 1`,
            [lastUpdate.dato, lastUpdate.oppdateringsid]
        );
    } catch (err) {
        console.log(err);
        throw err;
    }
}


async function getOppdaterteEnheter(dato: string, page: string, size: string): Promise<any> {
    try {
        const response = await axios.get(`https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter?dato=${dato}&page=${page}&size=${size}`);
        return response.data;
    } catch (error) {
        console.log(error);
        throw error;
    }
}

async function updateDatabase(dato: string, size: string) {
    let page = 0;
    let inputJson = await getOppdaterteEnheter(dato, page.toString(), size);
    while (page < inputJson.page.totalPages) {
        await addOppdaterteEnheter(inputJson);
        page++;
        inputJson = await getOppdaterteEnheter(dato, page.toString(), size);
    }
}


function findLastUpdate(inputJson: any): any {
    const oppdaterteEnheter = inputJson._embedded.oppdaterteEnheter;
    const lastRecord = oppdaterteEnheter[oppdaterteEnheter.length - 1];
    return { oppdateringsid: lastRecord.oppdateringsid, dato: lastRecord.dato };
}


async function getOneBrregEnhet(organisasjonsnummer: string): Promise<{ status: string, enhet?: any, message?: any }> {
    try {
        const response = await axios.get(`https://data.brreg.no/enhetsregisteret/api/enheter/${organisasjonsnummer}`);
        return { status: "success", enhet: response.data };
    } catch (err: any) {
        return { status: "error", message: err.stack };
    }
}



async function deleteBrregEnhet(oppdateringsid: number, dato: string, organisasjonsnummer: string, endringstype: string) {
    let theDate = new Date(dato);
    let theDateISO = theDate.toISOString();
    try {
        const checkOrganizationExist = await pool.query(`SELECT * FROM brreg_enheter_alle WHERE organisasjonsnummer = '${organisasjonsnummer}'`);
        if (checkOrganizationExist.rowCount === 0) {
            return { status: "notfound", message: `No organization with the number ${organisasjonsnummer} was found` };
        } else {
            const updateOrganization = await pool.query(`UPDATE brreg_enheter_alle SET urb_brreg_oppdateringsid = ${oppdateringsid}, urb_brreg_update_date = '${theDateISO}', urb_brreg_endringstype = '${endringstype}' WHERE organisasjonsnummer = '${organisasjonsnummer}'`);
            if (updateOrganization.rowCount === 1) {
                return { status: "success", message: "Organization successfully updated" };
            } else {
                return { status: "error", message: "Failed to update organization" };
            }
        }
    } catch (err: any) {
        return { status: "error", message: err.stack };
    }
}



async function updateBrregEnhet(jsonEnhet: any, jsonUpdate: any) {
    const dbEnhet = mapJSONEnhet2db(jsonEnhet);
    dbEnhet.urb_brreg_oppdateringsid = jsonUpdate.oppdateringsid;
    // convert date to ISO format
    let theDate = new Date(jsonUpdate.dato);
    dbEnhet.urb_brreg_update_date = theDate.toISOString();
    dbEnhet.urb_brreg_endringstype = jsonUpdate.endringstype;
    let updateQuery = `UPDATE brreg_enheter_alle SET `;
    const keys = Object.keys(dbEnhet);
    const values = Object.values(dbEnhet);
    try {
        const checkOrg = await pool.query(pgFormat("SELECT * FROM brreg_enheter_alle WHERE organisasjonsnummer = %L", jsonEnhet.organisasjonsnummer));
        if (checkOrg.rowCount > 0) {
            
            keys.forEach((key, i) => {
                if (key !== 'organisasjonsnummer') {
                    updateQuery += pgFormat("%I = %L, ", key, values[i]);
                }
            });
                        
            updateQuery = updateQuery.trim(); // remove ending spaces from updateQuery
            updateQuery = updateQuery.slice(0, -1); // remove ending comma from updateQuery
            updateQuery += pgFormat(" WHERE organisasjonsnummer = %L", jsonEnhet.organisasjonsnummer);
            const updateResult = await pool.query(updateQuery);
            return { status: "success", message: "Organization successfully updated" };
        } else {
            return { status: "notfound", message: "No organization found with that organisasjonsnummer" };
        }
    } catch (err: any) {
        return { status: "error", message: err.stack };
    }
}




async function createBrregEnhet(jsonEnhet: any, jsonUpdate: any): Promise<any> {
    let sql ="";
    try {
        const dbEnhet = mapJSONEnhet2db(jsonEnhet);
        const keys = Object.keys(dbEnhet);
        const values = Object.values(dbEnhet);
        // convert jsonUpdate.dato to ISO format
        const isoDate = new Date(jsonUpdate.dato).toISOString();
        jsonUpdate.dato = isoDate;

        // create the full sql statement using pgFormat
        sql = pgFormat(
            'INSERT INTO brreg_enheter_alle (%I, urb_brreg_oppdateringsid, urb_brreg_update_date, urb_brreg_endringstype) VALUES (%L, %L, %L, %L)',
            keys,
            values,
            jsonUpdate.oppdateringsid,
            jsonUpdate.dato,
            jsonUpdate.endringstype
        );
        // pass the sql statement to the pool.query function
        const result = await pool.query(sql);
        return { status: "success", message: "Brreg enhet was created" };
    } catch (err: any) {
        return { status: "error", message: err.message };
    }
}


async function getOneOppdatertEnhetet(organisasjonsnummer: string) {
    try {
        const result = await pool.query(`SELECT * FROM oppdaterteenheter WHERE organisasjonsnummer = '${organisasjonsnummer}'`);
        if (result.rows.length > 0) {
            return { status: "success", enhet: result.rows[0] };
        } else {
            return { status: "error", message: "No organization found with the given number" };
        }
    } catch (err: any) {
        return { status: "error", message: err.stack };
    }
}


async function updateOppdatertEnhet(oppdateringsid: number, status: string): Promise<{ status: string, message?: string }> {
    try {
        const currentDate = new Date();
        const result = await pool.query(`UPDATE oppdaterteenheter SET urb_processed = $1, urb_processed_status = $2 WHERE oppdateringsid = $3`, [currentDate, status, oppdateringsid]);
        if (result.rowCount === 0) {
            return { status: "notfound" };
        }
        return { status: "success" };
    } catch (err: any) {
        return { status: "error", message: err.stack };
    }
}

function mapJSONEnhet2db(jsonEnhet: any): any {
    let dbEnhet: any = {};

    dbEnhet.organisasjonsnummer = jsonEnhet.organisasjonsnummer;
    dbEnhet.navn = jsonEnhet.navn;
    dbEnhet.organisasjonsform_kode = jsonEnhet.organisasjonsform.kode ?? null;
    dbEnhet.organisasjonsform_beskrivelse = jsonEnhet.organisasjonsform.beskrivelse ?? null;
    dbEnhet.naringskode_1 = jsonEnhet?.naeringskode1?.kode ?? null;
    dbEnhet.naringskode_1_beskrivelse = jsonEnhet?.naeringskode1?.beskrivelse ?? null;
    dbEnhet.naringskode_2 = jsonEnhet?.naeringskode2?.kode ?? null;
    dbEnhet.naringskode_2_beskrivelse = jsonEnhet?.naeringskode2?.beskrivelse ?? null;
    dbEnhet.naringskode_3 = jsonEnhet?.naeringskode3?.kode ?? null;
    dbEnhet.naringskode_3_beskrivelse = jsonEnhet?.naeringskode3?.beskrivelse ?? null;
    //dbEnhet.hjelpeenhetskode: jsonEnhet.hjelpeenhets.kode,
    //dbEnhet.hjelpeenhetskode_beskrivelse: jsonEnhet.hjelpeenhetskode_beskrivelse,
    dbEnhet.antall_ansatte = jsonEnhet?.antallAnsatte ?? null;
    dbEnhet.hjemmeside = jsonEnhet?.hjemmeside ?? null;
    dbEnhet.postadresse_adresse = jsonEnhet?.postadresse?.adresse[0] ?? null;
    dbEnhet.postadresse_poststed = jsonEnhet?.postadresse?.poststed ?? null;
    dbEnhet.postadresse_postnummer = jsonEnhet?.postadresse?.postnummer ?? null;
    dbEnhet.postadresse_kommune = jsonEnhet?.postadresse?.kommune ?? null;
    dbEnhet.postadresse_kommunenummer = jsonEnhet?.postadresse?.kommunenummer ?? null;
    dbEnhet.postadresse_land = jsonEnhet?.postadresse?.land ?? null;
    dbEnhet.postadresse_landkode = jsonEnhet?.postadresse?.landkode ?? null;
    dbEnhet.forretningsadresse_adresse = jsonEnhet?.forretningsadresse?.adresse[0] ?? null;
    dbEnhet.forretningsadresse_poststed = jsonEnhet?.forretningsadresse?.poststed ?? null;
    dbEnhet.forretningsadresse_postnummer = jsonEnhet?.forretningsadresse?.postnummer ?? null;
    dbEnhet.forretningsadresse_kommune = jsonEnhet?.forretningsadresse?.kommune ?? null;
    dbEnhet.forretningsadresse_kommunenummer = jsonEnhet?.forretningsadresse?.kommunenummer ?? null;
    dbEnhet.forretningsadresse_land = jsonEnhet?.forretningsadresse?.land ?? null;
    dbEnhet.forretningsadresse_landkode = jsonEnhet?.forretningsadresse?.landkode ?? null;
    dbEnhet.institusjonell_sektorkode = jsonEnhet?.institusjonellSektorkode?.kode ?? null;
    dbEnhet.institusjonell_sektorkode_beskrivelse = jsonEnhet?.institusjonellSektorkode?.beskrivelse ?? null;
    dbEnhet.siste_innsendte_arsregnskap = jsonEnhet?.sisteInnsendteAarsregnskap ?? null;
    dbEnhet.registreringsdato_i_enhetsregisteret = jsonEnhet?.registreringsdatoEnhetsregisteret ?? null;
    dbEnhet.stiftelsesdato = jsonEnhet?.stiftelsesdato ?? null;
    dbEnhet.frivilligregistrertimvaregisteret = jsonEnhet?.registrertIFrivillighetsregisteret ?? null;
    dbEnhet.registrert_i_mva_registeret = jsonEnhet?.registrertIMvaregisteret ?? null;
    dbEnhet.registrert_i_frivillighetsregisteret = jsonEnhet?.registrertIFrivillighetsregisteret ?? null;
    dbEnhet.registrert_i_foretaksregisteret = jsonEnhet?.registrertIForetaksregisteret ?? null;
    dbEnhet.registrert_i_stiftelsesregisteret = jsonEnhet?.registrertIStiftelsesregisteret ?? null;
    dbEnhet.konkurs = jsonEnhet?.konkurs ?? null;
    dbEnhet.under_avvikling = jsonEnhet?.underAvvikling ?? null;
    dbEnhet.under_tvangsavvikling_eller_tvangsopplasning = jsonEnhet?.underTvangsavviklingEllerTvangsopplosning ?? null;
    dbEnhet.overordnet_enhet_i_offentlig_sektor = jsonEnhet?.overordnetEnhet ?? null;
    dbEnhet.malform = jsonEnhet?.maalform ?? null;

    return dbEnhet;
}

function printOppdatertEnhetLog(status: string) {
    switch (status) {
        case "success":
            console.log("Updated the oppdaterte_enheter table");
            break;
        case "error":
            console.log("Error: updating the oppdaterte_enheter table");
            break;
        case "notfound":
            console.log("notfound: Not found in the oppdaterte_enheter table");
            break;
        default:
            console.log("Unknown status");
            break;
    }
}


async function updateOneChange(jsonUpdate: IOppdaterteEnheter) {


    let jsonEnhet: IBrregEnheterAlle;
    let jsonEnhetResponse: {
        status: string;
        enhet?: any;
        message?: any;
    };

    let endringstype = jsonUpdate.endringstype;
    let updateOppdatertEnhetResponse;


    globalOppdaterteEnheter++;

    switch (endringstype) {

        case "Ukjent":
            console.log(globalOppdaterteEnheter + " Ukjent endringstype organisasjonsnummer=" + jsonUpdate.organisasjonsnummer);
            break;
        case "Ny":
            console.log(globalOppdaterteEnheter + " Ny endringstype organisasjonsnummer=" + jsonUpdate.organisasjonsnummer);
            jsonEnhetResponse = await getOneBrregEnhet(jsonUpdate.organisasjonsnummer);
            jsonEnhet = jsonEnhetResponse.enhet;
            let createBrregEnhetResponse = await createBrregEnhet(jsonEnhet, jsonUpdate);
             
            switch (createBrregEnhetResponse.status) {
                case "success":
                    console.log(globalOppdaterteEnheter + " Created the organization in the brreg_enheter_alle table");
                    updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, createBrregEnhetResponse.status);
                    printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                    break;

                case "error":
                    console.log(globalOppdaterteEnheter + " Error creating the organization in the brreg_enheter_alle table");
                    updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, createBrregEnhetResponse.status);
                    printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                    break;
                case "exists":
                    console.log(globalOppdaterteEnheter + " the organization already exists in the brreg_enheter_alle table");
                    updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, createBrregEnhetResponse.status);
                    printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                    break;
                default:
                    console.log(globalOppdaterteEnheter + " Unknown status");
                    updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, "Unknown status");
                    printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                    break;
            }

            break;
        case "Endring":
            console.log(globalOppdaterteEnheter + " Endring endringstype organisasjonsnummer=" + jsonUpdate.organisasjonsnummer);
            jsonEnhetResponse = await getOneBrregEnhet(jsonUpdate.organisasjonsnummer);
            switch (jsonEnhetResponse.status) {
                case "success":
                    jsonEnhet = jsonEnhetResponse.enhet;

                    updateOppdatertEnhetResponse = await updateBrregEnhet(jsonEnhet, jsonUpdate);
                    switch (updateOppdatertEnhetResponse.status) {
                        case "success":
                            console.log(globalOppdaterteEnheter + " Updated the brreg_enheter_alle table");
                            updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, updateOppdatertEnhetResponse.status);
                            printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                            break;

                        case "error":
                            console.log(globalOppdaterteEnheter + " Error updating the brreg_enheter_alle table");
                            updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, updateOppdatertEnhetResponse.status);
                            printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                            break;
                        case "notfound":
                            console.log(globalOppdaterteEnheter + " Not found in the brreg_enheter_alle table");
                            updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, updateOppdatertEnhetResponse.status);
                            printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                            break;
                        default:
                            console.log(globalOppdaterteEnheter + " Unknown status");
                            updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, "Unknown status");
                            printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                            break;
                    }


                    break;
                case "not found":
                    console.log(globalOppdaterteEnheter + " jsonEnhet not found organisasjonsnummer=" + jsonUpdate.organisasjonsnummer);
                    break;
                default:
                    break;
            }

            break;
        case "Sletting":
            console.log(globalOppdaterteEnheter + " Sletting endringstype organisasjonsnummer=" + jsonUpdate.organisasjonsnummer);
            let deleteBrregEnhetResponse = await deleteBrregEnhet(jsonUpdate.oppdateringsid, jsonUpdate.dato, jsonUpdate.organisasjonsnummer, jsonUpdate.endringstype);

            switch (deleteBrregEnhetResponse.status) {
                case "success":
                    console.log(globalOppdaterteEnheter + " Marked the organization Slettet in the brreg_enheter_alle table");
                    updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, deleteBrregEnhetResponse.status);
                    printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                    break;
                case "notfound":
                    console.log(globalOppdaterteEnheter + " Organization not found in the brreg_enheter_alle table");
                    updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, deleteBrregEnhetResponse.status);
                    printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                    break;
                case "Fjernet":
                    console.log(globalOppdaterteEnheter + " Fjernet endringstype");
                    updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, deleteBrregEnhetResponse.status);
                    printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                    break;
                default:
                    console.log(globalOppdaterteEnheter + " Unknown status");
                    updateOppdatertEnhetResponse = await updateOppdatertEnhet(jsonUpdate.oppdateringsid, "Unknown status");
                    printOppdatertEnhetLog(updateOppdatertEnhetResponse.status);
                    break;
            }




    }


}


async function processOppdaterteEnheter() {

    const recordsToProcess = 10;
    while (true) {

        try {
            //query to select 10 unprocessed records from oppdaterteenheter table
            const query = `SELECT * FROM oppdaterteenheter WHERE urb_processed IS NULL LIMIT ${recordsToProcess}`;
            const oppdaterteEnheter = await pool.query(query);
            if (oppdaterteEnheter.rowCount === 0) {
                break;
            }

            for (const enhet of oppdaterteEnheter.rows) {
                await updateOneChange(enhet);
            }
        } catch (err: any) {
            console.log("Error in processOppdaterteEnheter: ", err.stack);
        }
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
    /*
        const inputJson = {
            "_embedded": {
                "oppdaterteEnheter": [
                    {
                        "oppdateringsid": 16666472,
                        "dato": "2023-01-17T05:01:31.544Z",
                        "organisasjonsnummer": "814441822",
                        "endringstype": "Sletting",
                        "_links": {
                            "enhet": {
                                "href": "https://data.brreg.no/enhetsregisteret/api/enheter/814441822"
                            }
                        }
                    },
                    {
                        "oppdateringsid": 16666474,
                        "dato": "2023-01-17T05:01:31.544Z",
                        "organisasjonsnummer": "818071892",
                        "endringstype": "Sletting",
                        "_links": {
                            "enhet": {
                                "href": "https://data.brreg.no/enhetsregisteret/api/enheter/818071892"
                            }
                        }
                    },
                    {
                        "oppdateringsid": 16666476,
                        "dato": "2023-01-17T05:01:31.544Z",
                        "organisasjonsnummer": "820722272",
                        "endringstype": "Sletting",
                        "_links": {
                            "enhet": {
                                "href": "https://data.brreg.no/enhetsregisteret/api/enheter/820722272"
                            }
                        }
                    }
                ]
            },
            "_links": {
                "first": {
                    "href": "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter?dato=2023-01-17T00:00:00.000Z&page=0&size=3"
                },
                "self": {
                    "href": "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter?dato=2023-01-17T00:00:00.000Z&page=0&size=3"
                },
                "next": {
                    "href": "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter?dato=2023-01-17T00:00:00.000Z&page=1&size=3"
                },
                "last": {
                    "href": "https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter?dato=2023-01-17T00:00:00.000Z&page=1027&size=3"
                }
            },
            "page": {
                "size": 3,
                "totalElements": 3084,
                "totalPages": 1028,
                "number": 0
            }
        };
    */

    /*       const jsonEnhet2 = {
               "organisasjonsnummer": "994228617",
               "navn": "HARPREET BANSAL", 
               "organisasjonsform": {
                   "kode": "ENK", 
                   "beskrivelse": "Enkeltpersonforetak", 
                   "_links": { "self": { "href": "https://data.brreg.no/enhetsregisteret/api/organisasjonsformer/ENK" } }
   
               }, "registreringsdatoEnhetsregisteret": "2009-06-18", 
               "registrertIMvaregisteret": false, 
               "naeringskode1": { "beskrivelse": "Utøvende kunstnere og underholdningsvirksomhet innen musikk",  "kode": "90.011" }, 
               "antallAnsatte": 0, 
               "forretningsadresse": 
               { "land": "Norge", "landkode": "NO", "postnummer": "0487", "poststed": "OSLO", "adresse": ["Kapellveien 53"], "kommune": "OSLO", "kommunenummer": "0301" },
                "institusjonellSektorkode": 
                { "kode": "8200", "beskrivelse": "Personlig næringsdrivende" }, 
                "registrertIForetaksregisteret": false,
                 "registrertIStiftelsesregisteret": false, 
                 "registrertIFrivillighetsregisteret": false,
                  "konkurs": false, "underAvvikling": false, "underTvangsavviklingEllerTvangsopplosning": false, "maalform": "Bokmål", "_links": { "self": { "href": "https://data.brreg.no/enhetsregisteret/api/enheter/994228617" } }
           };
   */
    /*
        const jsonUpdate = {
            "oppdateringsid": 16666472,
            "dato": "2023-01-17 05:01:31.544+00",
            "organisasjonsnummer": "814441822",
            "endringstype": "Sletting",
            "urb_processed": null
          };
    */
    //  console.log("initiating urbalurba_status table");
    //  await initiateUrbalurbaStatus();


    //   console.log("initiating oppdaterteenheter table");
    //   await initiateOppdaterteEnheter();

    //   console.log("initiating brreg_enheter_alle table");
    //   await initiateBrregEnheterAlle();

    //   console.log("Getting first records in the table");
    //   let firstRecords = await getOrganizations(firstOrganizationsQuery, numberOfOrganizations);
    //   displayRecords(firstRecords);

    //   console.log("Getting largest organizations");
    //   const largestOrganizations = await getOrganizations(largestOrganizationsQuery, numberOfOrganizations);
    //   displayRecords(largestOrganizations);

    //   console.log("Getting largest organizations with no website");
    //   const largestOrganizationsNoWeb = await getOrganizations(largestOrganizationsNoWebQuery, numberOfOrganizations);
    //   displayRecords(largestOrganizationsNoWeb);

    //   console.log("Getting duplicate websites");
    //   const duplicateWeb = await getOrganizations(duplicateWebQuery, numberOfOrganizations);
    //   displayRecords(duplicateWeb);

    //   console.log("Getting duplicate websites count");
    //   const duplicateWebCount = await getOrganizations(duplicateWebCountQuery, numberOfOrganizations);
    //   displayRecords(duplicateWebCount);

    //   console.log("Getting duplicate websites table");
    //   const duplicateWebTable = await getOrganizations(duplicateWebTableQuery, 10000);
    //   displayRecords(duplicateWebTable);


    //  console.log("Adding sync_date field");
    //  let sucessAdding = await addFieldToTable('brreg_enheter_alle', 'urb_sync_date', 'TIMESTAMP');


    //  console.log("Getting the previous date of updates");
    //  let lastUpdateDate = await getPreviousDate();
    //  console.log("Last update date: " + lastUpdateDate);



    //    console.log("Getting the oppdaterteEnheter from the API");
    //    let inputJson = await getOppdaterteEnheter(lastUpdateDate, '0', '5');
    //    console.log("Getting the last update date from the API");
    //    let lastUpdate = findLastUpdate(inputJson);
    //    console.log("Last update date: " + JSON.stringify(lastUpdate));

    //  console.log("updating urbalurba_status with updatePreviousDate");
    //  await updatePreviousDate(lastUpdate);

    //    console.log("Adding inputJson to the oppdaterteEnheter table");
    //    let sucessAddingJson = await addOppdaterteEnheter(inputJson);


    //    console.log("Getting all updates since last update date and storing them in the database");
    //    await updateDatabase(lastUpdateDate, "5");


    //    let deleteResult = await deleteBrregEnhet(16666472, "2023-01-17 05:01:31.544+00", "814441822", "Sletting");
    //    console.log("deleteResult: " + JSON.stringify( deleteResult));

    let organisasjonsnummer = "926121758";

    let jsonUpdateResponse = await getOneOppdatertEnhetet(organisasjonsnummer);

    if (jsonUpdateResponse.status === "success") {
        let didWeUpdate = await updateOneChange(jsonUpdateResponse.enhet);
    } else {
        console.log("jsonUpdateResponse=" + JSON.stringify(jsonUpdateResponse));
    }


}


console.log("Starting")
initiateUrbalurbaStatus();
//main();
//processOppdaterteEnheter();

console.log("Done")