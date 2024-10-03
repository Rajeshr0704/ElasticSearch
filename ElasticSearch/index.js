
const { Client } = require('@elastic/elasticsearch');
const fs = require('fs');
const csv = require('csv-parser');
// const { createCollection} = require('./functions/indexfunc');

// createCollection('Hash_Rajesh');

const esClient = new Client({
  node: 'http://localhost:9200', 
  auth: {
    username: 'elastic', 
    password: 'h9UOKuk=uYpS_DBU4Ckf' 
  },
  ssl: {
    rejectUnauthorized: false 
  }
});


const indexName = 'employee_data';


async function createIndex() {
    try {
      // Check if the index already exists
      const exists = await esClient.indices.exists({ index: indexName });
  
      if (!exists.body) {
        // If it doesn't exist, create the index
        await esClient.indices.create({
          index: indexName,
          body: {
            settings: {
              number_of_shards: 1,
              number_of_replicas: 0,
            },
            mappings: {
              properties: {
                Employee_ID: { type: 'integer' },
                Full_Name: { type: 'text' },
                Job_Title: { type: 'text' },
                Department: { type: 'text' },
                Gender: { type: 'text' },
                Ethnicity: { type: 'text' },
                Age: { type: 'integer' },
                Hire_Date: { type: 'date', format: 'yyyy-MM-dd' }, // Ensure dates are in 'YYYY-MM-DD' format
                Annual_Salary: { type: 'float' }
              }
            }
          }
        });
        console.log(`Index '${indexName}' created successfully.`);
      } else {
        console.log(`Index '${indexName}' already exists. Skipping creation.`);
      }
    } catch (error) {
      console.error('Error creating index:', error);
    }
  }
  

async function indexEmployeeData(filePath) {
  const employees = [];

  
  fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (row) => {
      const employee = {
        Employee_ID: parseInt(row['Employee_ID'], 10),
        Name: row['Name'],
        Department: row['Department'],
        Age: parseInt(row['Age'], 10),
        Salary: parseFloat(row['Salary']),
      };
      employees.push(employee);
    })
    .on('end', async () => {
     
      const body = employees.flatMap(doc => [{ index: { _index: indexName } }, doc]);

      try {
        const bulkResponse = await esClient.bulk({ refresh: true, body });

        if (bulkResponse.errors) {
          const erroredDocuments = [];
          bulkResponse.items.forEach((action, i) => {
            const operation = Object.keys(action)[0];
            if (action[operation].error) {
              erroredDocuments.push({
                status: action[operation].status,
                error: action[operation].error,
                operation: body[i * 2],
                document: body[i * 2 + 1]
              });
            }
          });
          console.log('Some documents failed to index:', erroredDocuments);
        } else {
          console.log(`Successfully indexed ${employees.length} employees.`);
        }
      } catch (error) {
        console.error('Bulk indexing failed:', error);
      }
    });
}


(async function () {
  try {

    await esClient.ping();
    console.log('Connected to Elasticsearch');

   
    await createIndex();


    const filePath = './employee_data.csv'; 
    await indexEmployeeData(filePath);

  } catch (error) {
    console.error('Error connecting to Elasticsearch:', error);
  }
})();
