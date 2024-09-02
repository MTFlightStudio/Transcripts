from google.cloud import speech_v1p1beta1 as speech

client = speech.SpeechClient()

audio = speech.RecognitionAudio(uri="gs://your-bucket/your-audio-file.flac")

config = speech.RecognitionConfig(
    encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
    language_code="en-US",
    enable_speaker_diarization=True,
    diarization_speaker_count=2,  # Adjust based on the number of speakers
)

response = client.recognize(config=config, audio=audio)

for result in response.results:
    print("Transcript: {}".format(result.alternatives[0].transcript))
    for word in result.alternatives[0].words:
        print("Word: {}, Speaker: {}".format(word.word, word.speaker_tag))
