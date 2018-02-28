## sudo code
# ex: key = ABC plaintext = HELLO
# formula: c = (p + k) % 26
# convert char in key to idx wrapping around
# if space in plaintext don't advance idx of key
# iter thru plaintext and key converting each char in plaintext and shift by idx of key
# advance to next letter in keyword only if char in plaintext is a letter
# perserve case


def get_ciphertext(key, pt):
    """ Convert plaintext into ciphertext based on key using Vigenere’s cipher """
    # TODO: fix when plaintext is 'I like you' and key is 'pandas' because of wrapping issue
    ciphertext = ''
    pt_len = len(pt)
    key_len = len(key)
    pt_pos = 0
    key_pos = 0
    for i in range(pt_len):
        if pt[pt_pos].isalpha() and pt[pt_pos].isupper():
            key_idx = key_pos % key_len
            idx_ptchar = ord(pt[pt_pos]) - 65
            idx_keychar = ord(key[key_idx]) - 65
            encrypt_char = chr(((idx_ptchar + idx_keychar) % 26)+ 65)
            ciphertext += encrypt_char
            pt_pos += 1
            key_pos += 1
        elif pt[pt_pos].isalpha() and pt[pt_pos].islower():
            key_idx = key_pos % key_len
            idx_ptchar = ord(pt[pt_pos]) - 97
            idx_keychar = ord(key[key_idx]) - 97
            encrypt_char = chr(((idx_ptchar + idx_keychar) % 26) + 97)
            ciphertext += encrypt_char
            pt_pos += 1
            key_pos += 1
        else:
            ciphertext += pt[pt_pos]
            pt_pos += 1

    return ciphertext


if __name__ == '__main__':
    while True:
        key = input('Enter alphabetical key: ')
        if key.isalpha():
            pt = input('Enter plaintext: ')
            print(get_ciphertext(key, pt))
            break
        else:
            print('ERROR: key must consist of alphabetical characters')

