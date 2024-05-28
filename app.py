import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from streamlit_agraph import agraph, Node, Edge, Config
from fuzzywuzzy import process

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("GraphFrameExample") \
    .getOrCreate()


st.markdown("Voir le code source sur [GitHub](https://github.com/abdoanss/graph-v) pour telecharger le fichier Excel et lancer l'application.")

# Load the Excel file
file_path = st.file_uploader("Upload your Excel file", type=["xlsx"])
edges_df = pd.read_excel(file_path, sheet_name='Edges')
nodes_df = pd.read_excel(file_path, sheet_name='Nodes')

# Ensure consistent data types for edges and nodes
edges_df = edges_df.astype(str)
nodes_df = nodes_df.astype(str)

# Rename 'Node ID' column to 'id' in nodes DataFrame
nodes_df = nodes_df.rename(columns={"Node ID": "id", "Name": "name"})

# Rename edge columns to match GraphFrame requirements
edges_df = edges_df.rename(columns={"From Name": "src", "To Name": "dst"})

# Convert DataFrames to Spark DataFrames
edges_spark_df = spark.createDataFrame(edges_df)
nodes_spark_df = spark.createDataFrame(nodes_df)

# Create GraphFrame
g = GraphFrame(nodes_spark_df, edges_spark_df)

# Titre et description
st.title("Graph Visualization with GraphFrames and NLP Integration")
st.write("""
Cette application permet de visualiser et d'interagir avec des graphes créés à partir de données de nœuds et d'arêtes. 
Utilisez la barre de recherche pour trouver des nœuds spécifiques, même si vous ne connaissez pas leur nom exact.
""")

# input par l'utilisateur
input_filter = st.text_input("Entrez un filtre pour les nœuds")

if input_filter:
    
    node_names = nodes_df['name'].tolist()
    closest_match, score = process.extractOne(input_filter, node_names)
    
    st.write(f"Correspondance la plus proche pour '{input_filter}': {closest_match} (avec certitude de: {score}%)")

    
    # filtrer les arêtes en fonction de la correspondance la plus proche
    filtered_edges = g.edges.filter(g.edges["src"].startswith(closest_match) | g.edges["dst"].startswith(closest_match))
    filtered_edges_pd = filtered_edges.toPandas()

    st.write(f"Edges filtered by: {closest_match}")
    st.write(filtered_edges_pd)

    # filtrer les nœuds en fonction des noms dans les arêtes filtrées
    filtered_node_names = set(filtered_edges_pd['src']).union(set(filtered_edges_pd['dst']))

    # Filter nodes based on the names in filtered edges
    filtered_nodes_pd = nodes_df[nodes_df['name'].isin(filtered_node_names)]

    # Création des objets Node et Edge pour AGraph
    nodes = []
    edges = []

    for _, row in filtered_nodes_pd.iterrows():
        nodes.append(Node(id=row['id'], label=row['name'], size=15))

    for _, row in filtered_edges_pd.iterrows():
        source_id = nodes_df[nodes_df['name'] == row['src']]['id'].values
        target_id = nodes_df[nodes_df['name'] == row['dst']]['id'].values
        if source_id and target_id:
            edges.append(Edge(source=source_id[0], target=target_id[0], label=row['Edge Type']))

    config = Config(
        width="100%",
        height=750,
        directed=True,
        nodeHighlightBehavior=True,
        highlightColor="#F7A7A6",
        collapsible=True
    )

    return_value = agraph(nodes=nodes, edges=edges, config=config)


    import streamlit as st

  # ... your existing code for processing node information ...

  # Improved table styling using CSS
    st.markdown("""
    <style>
    .dataframe {
      width: 100%;
      border: 1px solid #ddd;
      border-collapse: collapse;
    }
    .dataframe thead tr {
      background-color: #f2f2f2;
    }
    .dataframe th, .dataframe td {
      padding: 8px;
    }
    .dataframe th {
      text-align: left;
    }
    .dataframe td {
      text-overflow: ellipsis;
      overflow: hidden;
      white-space: nowrap;
    }
    </style>
    """, unsafe_allow_html=True)

    # Afficher les informations du nœud lorsqu'un nœud est cliqué
    if return_value is not None:
      st.write(f"Node clicked: {return_value}")
      node_info = nodes_df[nodes_df['id'] == return_value]
      if not node_info.empty:
        st.write("Node information:")
        # Afficher les informations du nœud dans un tableau
        filtered_info = node_info.dropna(axis=1).T.reset_index()
        filtered_info.columns = ['Attribute', 'Value']
        filtered_info = filtered_info[filtered_info['Value'] != 'nan']
        st.table(filtered_info)
else:
    filtered_edges = g.edges.filter(g.edges["src"].startswith('') | g.edges["dst"].startswith(''))
    filtered_edges_pd = filtered_edges.toPandas()

    filtered_node_names = set(filtered_edges_pd['src']).union(set(filtered_edges_pd['dst']))

    # Filter nodes based on the names in filtered edges
    filtered_nodes_pd = nodes_df[nodes_df['name'].isin(filtered_node_names)]

    # Create Node and Edge objects for AGraph
    nodes = []
    edges = []

    for _, row in filtered_nodes_pd.iterrows():
        nodes.append(Node(id=row['id'], label=row['name'], size=15))

    for _, row in filtered_edges_pd.iterrows():
        source_id = nodes_df[nodes_df['name'] == row['src']]['id'].values
        target_id = nodes_df[nodes_df['name'] == row['dst']]['id'].values
        if source_id and target_id:
            edges.append(Edge(source=source_id[0], target=target_id[0], label=row['Edge Type']))

    config = Config(
        width="100%",
        height=750,
        directed=True,
        nodeHighlightBehavior=True,
        highlightColor="#F7A7A6",
        collapsible=True
    )

    return_value = agraph(nodes=nodes, edges=edges, config=config)


    import streamlit as st

  # ... your existing code for processing node information ...

  # Improved table styling using CSS
    st.markdown("""
    <style>
    .dataframe {
      width: 100%;
      border: 1px solid #ddd;
      border-collapse: collapse;
    }
    .dataframe thead tr {
      background-color: #f2f2f2;
    }
    .dataframe th, .dataframe td {
      padding: 8px;
    }
    .dataframe th {
      text-align: left;
    }
    .dataframe td {
      text-overflow: ellipsis;
      overflow: hidden;
      white-space: nowrap;
    }
    </style>
    """, unsafe_allow_html=True)

    # Afficher les informations du nœud lorsqu'un nœud est cliqué
    if return_value is not None:
      st.write(f"Node clicked: {return_value}")
      node_info = nodes_df[nodes_df['id'] == return_value]
      if not node_info.empty:
        st.write("Node information:")
        # Afficher les informations du nœud dans un tableau
        filtered_info = node_info.dropna(axis=1).T.reset_index()
        filtered_info.columns = ['Attribute', 'Value']
        filtered_info = filtered_info[filtered_info['Value'] != 'nan']
        st.table(filtered_info)




