import streamlit as st
import pandas as pd

# Tags that show excerpts on record view
EXCERPT_TAGS = ["attorneys_fees", "public_meeting_requirement", "local_preemption", "private_right_of_action"]
TAGS_TO_SHOW_ON_RECORD_VIEW = ["attorneys_fees", "public_meeting_requirement", "local_preemption", "private_right_of_action"]

# Load data with caching
@st.cache_data

def load_data():
    data = pd.read_parquet('data/mcu_list.parquet.gz')

    tag_to_id = {}
    id_to_idx = {}

    for i, record in data.iterrows():
        for tag in record['tag_list']:
            tag_to_id.setdefault(tag, []).append(record['unique_id'])
        id_to_idx[record['unique_id']] = i
    
    # print tags loaded
    print(f"Tags loaded: {tag_to_id.keys()}")

    return data, tag_to_id, id_to_idx


def render_listing_page(data, tag_to_id):
    st.title("📚 Alabama Statutes Explorer")
    st.markdown("Select a legal tag below to view related statutes.")

    selected_tag = st.selectbox("🔖 Filter by tag", TAGS_TO_SHOW_ON_RECORD_VIEW)

    if selected_tag:
        filtered_ids = tag_to_id[selected_tag]
        filtered_data = data[data['unique_id'].isin(filtered_ids)]

        st.markdown(f"Showing {len(filtered_data)} results for: `{selected_tag}`")
        
        for _, record in filtered_data.iterrows():
            unique_id = record['unique_id']
            st.markdown(f"[{record['full_name']}](?id={unique_id})")

            if selected_tag in EXCERPT_TAGS:
                try:
                    tag_idx = list(record['tag_list']).index(selected_tag)
                    excerpt = record['tag_dict_list'][tag_idx].get('excerpt', '')
                    with st.expander("Excerpt"):
                        st.write(excerpt)
                except (ValueError, IndexError, KeyError):
                    pass

        with st.expander("🔍 View Raw Filtered Data"):
            st.dataframe(filtered_data)


def render_record_page(data, id_to_idx, record_unique_id):
    if st.button("⬅️ Back to listing", key="back_button_top"):
        st.query_params['id'] = None

    try:
        record_idx = id_to_idx[record_unique_id]
        record = data.iloc[record_idx]

        st.title(record['full_name'])

        # Meta info
        col1, col2, col3 = st.columns(3)
        col1.metric("📍 Jurisdiction", record['jurisdiction'])
        col2.metric("📅 Year", record['year'])

        # Process and display statute text
        lines = [line for line in record['text'].split('\n') if line.strip()]
        processed_lines = []
        skip_next = False
        
        for i in range(len(lines)):
            if skip_next:
                skip_next = False
                continue
            if lines[i].startswith('#') and i + 1 < len(lines):
                section = lines[i] + ": " + lines[i + 1]
                processed_lines.append(section)
                skip_next = True
            else:
                processed_lines.append(lines[i])

        formatted_text = '## ' + '\n\n'.join(processed_lines).replace('#', '##')

        # Escape $ in formatted_text
        formatted_text = formatted_text.replace('$', '\\$')
        st.markdown(formatted_text)

        # Tags
        with st.expander("🏷️ Tags"):
            st.write(record['tag_list'])

        if st.button("⬅️ Back to listing", key="back_button_bottom"):
            st.query_params['id'] = None

    except (ValueError, IndexError):
        st.error("🚫 Record not found.")


def main():
    data, tag_to_id, id_to_idx = load_data()
    record_unique_id = st.query_params.get("id")

    if not record_unique_id or record_unique_id == 'None':
        render_listing_page(data, tag_to_id)
    else:
        render_record_page(data, id_to_idx, record_unique_id)


if __name__ == "__main__":
    main()
