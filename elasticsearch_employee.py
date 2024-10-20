import csv
from elasticsearch import Elasticsearch

# Initialize the Elasticsearch client
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=('elastic', 'N+omuMad8n4S8OMHsjRG'),
    verify_certs=False
)

# Collection names
v_nameCollection = 'hash_dinesh'
v_phoneCollection = 'hash_8409'

# Create a collection (index)
def create_collection(collection_name):
    try:
        es.indices.create(index=collection_name, ignore=400)
        print(f"Index '{collection_name}' created.")
    except Exception as e:
        print(f"Error creating index: {e}")

# Index data from CSV
def index_data(collection_name, csv_file_path):
    with open(csv_file_path, mode='r', encoding='latin1') as file:  # Change to 'latin1'
        reader = csv.DictReader(file)
        for row in reader:
            es.index(index=collection_name, document=row)
        print(f"Data indexed into '{collection_name}'.")

# Get employee count
def get_emp_count(collection_name):
    return es.count(index=collection_name)['count']

# Delete employee by ID
def del_emp_by_id(collection_name, emp_id):
    try:
        es.delete(index=collection_name, id=emp_id)
        print(f"Employee with ID '{emp_id}' deleted.")
    except Exception as e:
        print(f"Error deleting employee: {e}")

# Search by column
def search_by_column(collection_name, column_name, value):
    query = {
        "query": {
            "match": {
                column_name: value
            }
        }
    }
    response = es.search(index=collection_name, query=query)
    print(f"Search results for '{column_name}' = '{value}':")
    for hit in response['hits']['hits']:
        print(hit['_source'])

# Get department facet
def get_dep_facet(collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"  # Use the correct field for aggregation
                }
            }
        }
    }
    response = es.search(index=collection_name, body=query)
    print("Department Facets:")
    for bucket in response['aggregations']['department_count']['buckets']:
        print(f"{bucket['key']}: {bucket['doc_count']}")

# Main execution
def main():
    create_collection(v_nameCollection)
    create_collection(v_phoneCollection)

    # Index data from CSV
    csv_file_path = r'C:\Users\User\Downloads\Employee Sample Data 1.csv'
    index_data(v_nameCollection, csv_file_path)

    # Execute the required functions in order
    print("Employee count in 'hash_dinesh':", get_emp_count(v_nameCollection))
    index_data(v_phoneCollection, csv_file_path)  # Index gender data
    del_emp_by_id(v_nameCollection, 'E02003')  # Delete employee by ID
    print("Employee count after deletion:", get_emp_count(v_nameCollection))
    
    search_by_column(v_nameCollection, 'Department', 'IT')
    search_by_column(v_nameCollection, 'Gender', 'Male')
    search_by_column(v_phoneCollection, 'Department', 'IT')
    get_dep_facet(v_nameCollection)
    get_dep_facet(v_phoneCollection)

if __name__ == "__main__":
    main()
