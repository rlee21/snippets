

def get_ciphertext(key, plaintext):
    """ Convert plaintext into ciphertext based on key using Vigenere’s cipher """
    #formula = (plaintext + key) % 26
    # TODO: make case insensitive
    # TODO: fix to handle spaces e.g. key='bacon' and plaintext='Meet me at the park at eleven am'
    letters = 'abcdefghijklmnopqrstuvwxyz'
    alpha2nums = {}
    data = list(zip(letters, [i for i in range(26)]))
    for item in data:
        alpha2nums[item[0]] = item[1]
    nums2alpha = {}
    for k, v in alpha2nums.items():
        nums2alpha[v] = k
    p_len = len(plaintext)
    k_len = len(key)
    key_nums = [i % k_len for i in range(p_len)]
    ciphertext = ''
    for idx, i in enumerate(plaintext):
        ciphernum = alpha2nums[i] + key_nums[idx]
        char = nums2alpha[ciphernum]
        ciphertext += char 
    
    return ciphertext


if __name__ == '__main__':
    key = input('Enter key: ')
    for char in key:
        if not char.isalpha():
            raise TypeError('key must consist of alphabetic characters only')
    plaintext = input('Enter plaintext: ')
    print(get_ciphertext(key, plaintext))
