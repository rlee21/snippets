"""
Given an array of integers, return indices of the two numbers such that they add up to a specific target.
You may assume that each input would have exactly one solution, and you may not use the same element twice.
"""


def two_sum1(nums, target):
    output = []
    for i in range(len(nums)):
        for j in range(len(nums)):
            value = nums[i] + nums[j]
            if value == target and i != j:
                output.append(i)
                output.append(j)
                return output


def two_sum2(nums, target):
    hash_map = {}
    for idx, num in enumerate(nums):
        if target - num in hash_map:
            return [hash_map[target - num], idx]
        else:
            hash_map[num] = idx


if __name__ == '__main__':
    nums = [-234, 43, 545, 2, 5, 345, 7]
    target = 12
    # nums = [3, 2, 4]
    # target = 6
    # print(two_sum1(nums, target))
    print(two_sum2(nums, target))