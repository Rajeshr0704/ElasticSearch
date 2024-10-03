const {Client} = require('@elastic/elasticsearch');

// initialize elasticserach

//const esClient = new Client({node: 'http://localhost:9200'});

//create a collection index
const esClient = new Client({ 
    node: 'http://localhost:9200', 
    auth: {
        username: 'elastic',  // Replace with your username
        password: 'h9UOKuk=uYpS_DBU4Ckf'
    }
});

async function createCollection(p_collection_name) {
   // console.log(`Attempting to create index: ${p_collection_name}`);

    try{

        const exists = await esClient.indices.exists({ index: p_collection_name });
        
        await esClient.indices.create({
            index:p_collection_name
        });
        console.log(`Index '${p_collection_name}' created successfully...`);
    }catch(error){
        console.error(`Error creating index:`,error);
    }
    
}

//index data

async function indexData(p_collection_name,p_exclude_column) {
    const employeeData =[
        { EmployeeID:'E02001',FullName:'Jone Doe',JobTitle:'Developer',Department:'IT',Age: 25,Gender:'Male'},
        { EmployeeID:'E02002',FullName:'Jone Smith',JobTitle:'Hr',Department:'Manager',Age: 35,Gender:'Female'},
        { EmployeeID:'E02003',FullName:'Jone',JobTitle:'Developer',Department:'IT',Age: 30,Gender:'Male'},
    ];

    try{
        for(const employee of employeeData){
            const dataToIndex  ={...employee};
            delete dataToIndex[p_exclude_column];
            await esClient.index({
                index:p_collection_name,
                id:employee.EmployeeID,
                body:dataToIndex
            });
        }
        console.log(`Successfully indexed data into '${p_collection_name}',excluding '${p_exclude_column}'..`);
    }catch(error){
        console.error(`Error indexing data:`,error);
    }
    
}
async function searchByColumn(p_collection_name,p_column_name,p_column_value) {
    try{
        const result = await esClient.search({
            index:p_collection_name,
            body:{
                query:{
                    match:{
                        [p_column_name]:p_column_value
                    }
                }
            }
        });
        console.log(`Search results for '${p_column_name}: ${p_column_value}' in '${p_collection_name}':`,result.hits.hits);
    }catch(error){
        console.error(`Error searching by column`,error);
    }
    
}

async function getEmpCount(p_collection_name) {
    try{
        const result = await esClient.count({
            index:p_collection_name
        });
        console.log(`Employee count in '${p_collection_name}:'`,result.count);
    }catch(error){
        console.error(`Error getting employee count:`,error);
    }
    
}

async function deleteById(p_collection_name,p_employee_id) {
    try{
        await esClient.delete({
            index:p_collection_name,
            id:p_employee_id
        });
        console.log(`Employee with ID '${p_employee_id}'deleted from '${p_collection_name}'`);
    }catch(error){
        console.error(`Error deleting employee:`,error);
    } 
}

async function getDepFacet(p_collection_name) {
   try{
        const result = await esClient.search({
            index:p_collection_name,
            body:{
                size:0,
                aggs:{
                    departments:{
                        terms:{
                            field:"Department.keyword"
                        }
                    }
                }

            }
        });
        console.log(`Department facet for '${p_collection_name}:'`,result.aggregations.departments.buckets);
   }catch(error){
    console.error(`Error retrieving department facet:`,error);
   }
    
}

(async () => {
    console.log('Starting script...'); // Log when the script starts

    const v_nameCollection='hash_rajesh';
    const v_phoneCollection='hash_0733';
    await createCollection('hash_rajesh');
    await createCollection('hash_0733');


    await indexData(v_nameCollection,'Department');
    await indexData(v_phoneCollection,'Gender');

    await deleteById(v_nameCollection,'E02003');


    await getEmpCount(v_nameCollection)


    await searchByColumn(v_nameCollection,'Department','IT');
    await searchByColumn(v_nameCollection,'Gender','Male');
    await searchByColumn(v_phoneCollection,'Department','IT');


    await getDepFacet(v_nameCollection);
    await getDepFacet(v_phoneCollection);
})();