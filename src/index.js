// src/index.js

// Import necessary modules
const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

// Function to evaluate conditions in WHERE clause
function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

// Main function to execute SELECT query
async function executeSELECTQuery(query) {
    // Parse the query
    const { fields, table, whereClauses, joinTable, joinCondition } = parseQuery(query);
    
    // Read data from the main table
    let data = await readCSV(`${table}.csv`);
    
    // Perform INNER JOIN if specified
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        data = data.flatMap(mainRow => {
            return joinData
                .filter(joinRow => {
                    // Check if both fields exist in the rows before comparing
                    if (mainRow.hasOwnProperty(joinCondition.left.split('.')[1]) && 
                        joinRow.hasOwnProperty(joinCondition.right.split('.')[1])) {
                        const mainValue = mainRow[joinCondition.left.split('.')[1]];
                        const joinValue = joinRow[joinCondition.right.split('.')[1]];
                        return mainValue === joinValue;
                    }
                    return false;
                })
                .map(joinRow => {
                    return fields.reduce((acc, field) => {
                        // Check if the field includes table prefix
                        if (field.includes('.')) {
                            const [tableName, fieldName] = field.split('.');
                            acc[fieldName] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                        } else {
                            acc[field] = mainRow[field] !== undefined ? mainRow[field] : joinRow[field];
                        }
                        return acc;
                    }, {});
                });
        });
    }
    
    // Apply WHERE clause filtering after JOIN (or on the original data if no join)
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;

    // Prepare the selected data
    const selectedData = filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            // Check if the field includes table prefix
            if (field.includes('.')) {
                const [tableName, fieldName] = field.split('.');
                selectedRow[fieldName] = row[fieldName];
            } else {
                selectedRow[field] = row[field];
            }
        });
        return selectedRow;
    });

    return selectedData;
}

module.exports = executeSELECTQuery;
