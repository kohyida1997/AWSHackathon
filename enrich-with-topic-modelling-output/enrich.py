import pandas as pd

def getDocnameAddedCsv(filePath, docnamePrefix):
    temp = pd.read_csv(filePath)
    temp['docname'] = docnamePrefix + temp.index.astype(str)
    return temp

def getDocnamePrefixFromDocTopicsOutput(topicModellingDocTopicsOutputFilePath):
    docname_1k = pd.read_csv(topicModellingDocTopicsOutputFilePath)
    idx_to_split = docname_1k['docname'].loc[0].find(':')
    return docname_1k['docname'].loc[0][:idx_to_split + 1]

def createEnrichedCSVWithTextTimestampsAndTopics(textAndTimestampOnlyFilePath, topicModellingDocTopicsOutputFilePath):
    docname_prefix = getDocnamePrefixFromDocTopicsOutput(topicModellingDocTopicsOutputFilePath)
    temp_csv_with_docnames = getDocnameAddedCsv(textAndTimestampOnlyFilePath, docname_prefix)
    topicModellingDocTopicsCsv = pd.read_csv(topicModellingDocTopicsOutputFilePath)
    dominant_topic_key = 'dominant_topic'
    proportion_key = 'proportion'

    temp_csv_with_docnames[dominant_topic_key] = -1
    temp_csv_with_docnames[proportion_key] = -1
    
    for index, row in temp_csv_with_docnames.iterrows():
        docname_for_this_row = row['docname']
        id_max = topicModellingDocTopicsCsv[topicModellingDocTopicsCsv['docname'] == docname_for_this_row][['proportion']].idxmax()
        dominant_topic_for_this_row = int(topicModellingDocTopicsCsv.loc[id_max]['topic'])
        proportion_for_this_dominant_topic = float(topicModellingDocTopicsCsv.loc[id_max]['proportion'])
        temp_csv_with_docnames.at[index, dominant_topic_key] = dominant_topic_for_this_row
        temp_csv_with_docnames.at[index, proportion_key] = proportion_for_this_dominant_topic
    return temp_csv_with_docnames

# Change this accordingly
textAndTimeStampsOriginalInputFilePath = 'text-timestamp-questions-only-title.csv'

# Change this accordingly
topicModellingDocTopicsOutputFilePath = 'TOPICS-output-66f3abb8a7275e615b13d49c58b75824-doc-topics.csv'

enrichedCSVWithTextTimestampsAndTopics = (
    createEnrichedCSVWithTextTimestampsAndTopics(
        textAndTimeStampsOriginalInputFilePath, topicModellingDocTopicsOutputFilePath))

enrichedCSVFilePath = textAndTimeStampsOriginalInputFilePath.replace('.csv', '-enriched.csv')

enrichedCSVWithTextTimestampsAndTopics.to_csv(enrichedCSVFilePath, index=False)