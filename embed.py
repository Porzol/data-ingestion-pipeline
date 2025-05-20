from sentence_transformers import SentenceTransformer
import hdbscan

model = SentenceTransformer('all-MiniLM-L6-v2')
clusterer = hdbscan.HDBSCAN(min_cluster_size=5)

def get_labels(embeddings):
    return clusterer.fit_predict(embeddings)

def add_embeddings(conversation:dict):
    vectors = model.encode(conversation['conversation_history']).tolist()
    conversation["vectors"] = vectors

    return conversation