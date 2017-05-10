from google.cloud import language

def lang_analysis(text):
    client = language.Client()
    document = client.document_from_text(text)
    sent_analysis = document.analyze_sentiment()
    ent_analysis = document.analyze_entities()
    sentiment = sent_analysis.sentiment
    entities = ent_analysis.entities
    #print(dir(sent_analysis))
    return sentiment, entities

example_text = "Callaway makes amazing custom golf clubs"
example_text = "Callaway is the worst golf company"
example_text = "Callaway does a good job with club drivers but an OK job with club putters"

sentiment, entities = lang_analysis(example_text)
print(sentiment.score)

for e in entities:
    print(e.name, e.entity_type, e.matadata, e.salience)


