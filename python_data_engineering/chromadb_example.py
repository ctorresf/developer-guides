# This is a simple example of how to use ChromaDB, a vector database, to store and query text data based on semantic similarity.
# to run this code, make sure you have the chromadb library installed. You can install it using pip:
# pip install chromadb==1.5.5
# or running:
# pip install -r requirements_chromadb.txt

import chromadb
from chromadb.utils import embedding_functions

# 1. Client and embedding model configuration
# We will use the default ChromaDB model (Sentence Transformers)
client = chromadb.Client()

# We created a collection called "quickride_policies"
# Collections are the equivalent of "tables" in SQL
collection = client.create_collection(name="quickride_policies")

# 2. Test Data: QuickRide Policy Manual
# We define the documents (paragraphs from the manual)
documents_manual = [
    "Cancellation Policy: If the passenger cancels after 5 minutes of the scheduled pickup, a $2 USD fee will apply.",
    "Cancellation Policy: If the driver does not arrive at the pickup location after a 10-minute wait, the user can cancel the trip free of charge.",
    "Dynamic Pricing: Prices may increase by 20% during peak hours or adverse weather conditions.",
    "Safety: All drivers must pass biometric verification before starting their shift.",
    "Lost Items: QuickRide is not responsible for items left in the vehicle, but will facilitate contact with the driver."
]

# Metadata to filter or give additional context (optional)
metadata = [
    {"category": "payments"},
    {"category": "payments"},
    {"category": "prices"},
    {"category": "security"},
    {"category": "support"}
]

# Unique IDs for each document
ids = ["id1", "id2", "id3", "id4", "id5"]

# 3. Data insertion (This is where embedding happens automatically)
print("Inserting documents into the Vector DB...")
collection.add(
    documents=documents_manual,
    metadatas=metadata,
    ids=ids
)

# 4. Problem Solving: Semantic Query
#query = "What is the trip cancellation policy?"
#query = "What are the rules for aborting a ride request?"
#query = "I don't want to go anymore, what happens?"
#query = "What if I change my mind after the driver was assigned?"
query = "What is the no-show protocol for passengers?"

print(f"\nPerforming a search for: '{query}'")
results = collection.query(
    query_texts=[query],
    n_results=2  # We requested the 2 closest results
)

# 5. Show results
print("\n--- Found Results ---")
for i in range(len(results['documents'][0])):
    doc = results['documents'][0][i]
    distancia = results['distances'][0][i]
    print(f"Result {i+1} (Distance: {round(distancia, 4)}):")
    print(f"Content: {doc}\n")

# --- Embedding Visualization ---
# We obtain the embedding of the first document (index 0)
embedding_example = collection.get(ids=["id1"], include=["embeddings"])['embeddings'][0]

print("\n--- Embedding Visualization ---")
print(f"The text was converted into a vector of {len(embedding_example)} dimensions.")
print(f"First 5 values of the vector: {embedding_example[:5]}")