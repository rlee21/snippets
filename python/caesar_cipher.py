

def get_ciphertext(key, plaintext):
    """ Convert plaintext into ciphertext based on key using Caesar’s cipher """
    ciphertext = ''
    for char in plaintext:
        if char.isalpha() and char.isupper():
            idx = ord(char) - 65
            new_char = chr(((idx + key) % 26) + 65)
            ciphertext += new_char
        elif char.isalpha() and char.islower():
            idx = ord(char) - 97
            new_char = chr(((idx + key) % 26) + 97)
            ciphertext += new_char
        else:
            ciphertext += char
    
    return ciphertext


if __name__ == '__main__':
    key = int(input('Enter key: '))
    plaintext = input('Enter plaintext: ')
    print(get_ciphertext(key, plaintext))
