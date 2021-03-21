from itertools import chain, cycle

import numpy as np
from transformers import GPT2TokenizerFast

END_OF_SPEAKER_1_TOKEN = '[END_OF_SPEAKER_1]'
END_OF_SPEAKER_2_TOKEN = '[END_OF_SPEAKER_2]'
SPECIAL_TOKENS = [END_OF_SPEAKER_1_TOKEN, END_OF_SPEAKER_2_TOKEN]


class DialogsTokenizer:
    def __init__(self, tokenizer_name_or_path, max_n_tokens, max_n_utterances):
        self._tokenizer = GPT2TokenizerFast.from_pretrained(tokenizer_name_or_path)

        self._max_n_tokens = max_n_tokens
        self._max_n_utterances = max_n_utterances

        self._tokenizer.add_special_tokens({'additional_special_tokens': SPECIAL_TOKENS})
        self._dtype = np.uint16 if self._tokenizer.vocab_size < 65500 else np.int32
        self._end_of_speaker_1_token_id = self._tokenizer.convert_tokens_to_ids(END_OF_SPEAKER_1_TOKEN)
        self._end_of_speaker_2_token_id = self._tokenizer.convert_tokens_to_ids(END_OF_SPEAKER_2_TOKEN)

    @property
    def pad_token_id(self):
        return self._tokenizer.eos_token_id

    @property
    def end_of_speaker_1_token_id(self):
        return self._end_of_speaker_1_token_id

    @property
    def end_of_speaker_2_token_id(self):
        return self._end_of_speaker_2_token_id

    @property
    def vocab_size(self):
        return max(self._tokenizer.all_special_ids) + 1

    @property
    def max_n_tokens(self):
        return self._max_n_tokens

    @property
    def max_n_utterances(self):
        return self._max_n_utterances

    def encode_dialog(self, dialog, encode_for_inference=False):
        encoded_utterances = [self.encode_utterance(utterance) for utterance in dialog]
        encoded_utterances = encoded_utterances[-self._max_n_utterances:]

        if encode_for_inference:
            end_of_speaker_input_ids = (self.end_of_speaker_2_token_id, self.end_of_speaker_1_token_id)
        else:
            end_of_speaker_input_ids = (self.end_of_speaker_1_token_id, self.end_of_speaker_2_token_id)

        if len(encoded_utterances) % 2 == 0:
            end_of_speaker_input_ids_cycle = cycle(end_of_speaker_input_ids)
        else:
            end_of_speaker_input_ids_cycle = cycle(reversed(end_of_speaker_input_ids))

        utterance_lengths = []
        for end_of_speaker_token_id, encoded_utterance in zip(end_of_speaker_input_ids_cycle, encoded_utterances):
            encoded_utterance.append(end_of_speaker_token_id)
            utterance_lengths.append(len(encoded_utterance))

        # total length = length on all N utterances
        total_length = sum(map(len, encoded_utterances))

        # If total length is larger than maximum allowed, drop utterances from left untill appropriate
        # total length will be reached:
        drop_n_first_utterances = 0
        while total_length > self._max_n_tokens:
            total_length -= len(encoded_utterances[drop_n_first_utterances])
            drop_n_first_utterances += 1

        if drop_n_first_utterances >= len(encoded_utterances) - 1:
            is_incomplete = True
            encoded_utterances = encoded_utterances[-1:]
        else:
            is_incomplete = False
            encoded_utterances = encoded_utterances[drop_n_first_utterances:]

        encoded_utterances = encoded_utterances[-self._max_n_utterances:]

        n_utterances = len(encoded_utterances)

        encoded_dialog = list(chain(*encoded_utterances))
        encoded_dialog = encoded_dialog[-self._max_n_tokens:]
        encoded_dialog = np.array(encoded_dialog, dtype=self._dtype)

        return encoded_dialog, n_utterances, is_incomplete

    def encode_utterance(self, utterance):
        utterance = utterance.capitalize()
        encoded_utterance = self._tokenizer.encode(utterance, add_special_tokens=False)
        return encoded_utterance

    def decode(self, input_ids):
        return self._tokenizer.decode(input_ids, skip_special_tokens=True)
