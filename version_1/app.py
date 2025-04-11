import streamlit as st
import pandas as pd


# Load your data (this could be from a database, CSV, etc.)
@st.cache_data
def load_data():
    # Load the data
    data = pd.read_parquet('data/mcu_list.parquet.gz') 
    
    # Construct a mapping from effect to list of (unique id, explanation) tuples.
    # Also construct a mapping from unique id to dataframe row idx
    effect_to_id = {}
    id_to_idx = {}
    for i in range(len(data)):
        record = data.iloc[i]
        for effect in record['legal_effects']:
            if effect['effect'] not in effect_to_id:
                effect_to_id[effect['effect']] = []
            effect_to_id[effect['effect']].append((record['unique_id'], effect['explanation']))
            id_to_idx[record['unique_id']] = i
    
    
    # Construct a list of unique effects
    unique_effects = list(effect_to_id.keys())
    unique_effects.sort()
    return data, effect_to_id, unique_effects, id_to_idx

def main():
    # Get the data
    data, effect_to_id, unique_effects, id_to_idx = load_data()
    
    # Get query parameters
    query_params = st.query_params
    record_unique_id = query_params.get("id", None)
    print(record_unique_id)
    if record_unique_id is None:
        # This is the index/listing page
        st.title("Alabama Statutes Demo")
        
        # Filter by effect
        selected_effect = st.selectbox("Filter by effect", unique_effects)
        
        if selected_effect:
            # Filter data based on selected effect
            filtered_ids, filtered_explanations = zip(*effect_to_id[selected_effect])
            filtered_data = data[data['unique_id'].isin(filtered_ids)]
            
            # Display the list of records with links
            st.write(f"Showing {len(filtered_data)} records for effect: {selected_effect}")
            
            for i in range(len(filtered_data)):
                record = filtered_data.iloc[i]
                unique_id = record['unique_id']
                st.markdown(f"[{record['full_name']}](?id={unique_id})")
                st.markdown(f"Unique ID: {unique_id}")
                st.markdown(f"**Explanation:** {filtered_explanations[i]}")
                st.markdown("---")
    else:
        # This is an individual record page
        try:
            record_idx = id_to_idx[record_unique_id]
            record = data.iloc[record_idx]
            
            # Display the record
            st.title(f"{record['full_name']}")
            
            # Pretty rendering of the statute
            st.write(f"**Jurisdiction:** {record['jurisdiction']}")
            st.write(f"**Year:** {record['year']}")
            st.write(f"**Path:** {record['path']}")

            # the statute text is a bit funky, where the section numbers are preceded by a "#" symbol but the section name is on the following line.
            # we want to split the text into lines and then for each line, if the previous line is a section number, we want to add it to the section number.
            lines = record['text'].split('\n')
            
            # Remove empty lines
            lines = [line for line in lines if line.strip()]

            for i in range(len(lines)):
                if lines[i].startswith('#'):
                    #lines[i] = lines[i].replace('#', '')
                    lines[i] = lines[i] + ": " + lines[i+1]
                    lines[i+1] = ''
            text = '\n\n'.join(lines)

            # We also want to replace "#" with "##" in the text
            text = text.replace('#', '##')
            st.markdown(text)
            st.subheader("# Legal Effects")
            for effect in record['legal_effects']:
                st.write(f"**Effect:** {effect['effect']}")
                st.write(f"**Explanation:** {effect['explanation']}")
                st.write(f"**Sections:** {', '.join(effect['sections'])}")
                st.write("---")
                
            # Add a back button
            st.markdown("[Back to listing](?)")
        except (ValueError, IndexError):
            st.error("Record not found")

if __name__ == "__main__":
    main()