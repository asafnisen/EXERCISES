
"""
1
two_sums
Input: nums = [2,7,11,15], target = 9
Output: [0,1]
Explanation: Because nums[0] + nums[1] == 9, we return [0, 1].
"""

def two_sums (L,target):
    L_1 = []
    L_2 = []
    for num in range(len(L)):
       for num2 in range(num+1,len(L)):
           if L[num]+L[num2] == target :
               L_1.append(num)
               L_1.append(num2)
               L_2.append(L_1)
               L_1 = []
    return (L_2)

#L=[2,7,2,15]
#target = 9
#asaf = two_sums(L,target)
#print (asaf)

"""
2
Add Two Numbers
Input: l1 = [2,4,3], l2 = [5,6,4]
Output: [7,0,8]
Explanation: 342 + 465 = 807.
"""

def list_to_num(L1):
    L1_str = ','.join(map(str, L1))[::-1]
    L1_str = int(L1_str.replace(",", ""))
    return(L1_str)

def num_to_lost(num):
    my_list = []
    str_num = (str(num)[::-1])
    for number in str_num:
        my_list.append(int(number))
    return(my_list)


#L1 = [2,4,3]
#l2 = [5,6,4]
#print(num_to_lost((list_to_num(L1) + (list_to_num(l2)))))

"""
3
Longest Substring Without Repeating Characters
Input: s = "abcabcbb"
Output: 3
Explanation: The answer is "abc", with the length of 3.
"""

def string_to_list (s):
    L_fina=[]
    L_temp=[]
    x=len(s)
    i=1
    for letter in s:
        if letter not in L_temp:
            L_temp.append(letter)
            if i==x: # last letter
                L_fina.append(''.join(L_temp))
                L_temp = []
                L_temp.append(letter)
        else:
            L_fina.append(''.join(L_temp))
            L_temp = []
            L_temp.append(letter)
        if i == x: # last letter
            L_fina.append(''.join(L_temp))
        i+=1
    return L_fina
    #print (max(L_fina, key=len)) # לא מחזיר אם יש כמה

def max_len_list (L_fina):
    s= map(lambda x : len(x),L_fina)
    L=zip(list(s),L_fina)
    newlist = [item[0] for item in list(L)]
    return (max(newlist))

#b = string_to_list('sdfdfskwfmabggddd')
#a = max_len_list(b)
#for item in b:
#    if len(item)== a:
#        print (item)

"""
4
Longest Palindromic Substring
Input: s = "babad"
Output: "bab"
Explanation: "aba" is also a valid answer.
"""
def check_Palindromic(s):
    answer=False
    if s[::-1]==s:
        answer=True
    return answer

def check_Palindromic_main(s):
    L_s=list(s)
    print (L_s)
    i=1
    L_poli = []
    for letter in L_s:
        L = []
        L.append(letter)
        for num in range (i,len(s)):
            L.append(L_s[num])
            str =''.join(L)
            answer_pol=check_Palindromic(str)
            if answer_pol == True:
                L_poli.append(str)
        i+=1
    s = map(lambda x: len(x), L_poli) # check the biggest
    max_len= (max(list(s)))
    return [str for str in L_poli if len(str) == max_len]
    #return [[str,i] for i,str in enumerate(L_poli) if len(str) == max_len] # עם מיקום אינדקס

#s = "babad"
#b= check_Palindromic_main(s)
#print (b)

"""
5
Reverse Integer
Input: x = 123
Output: 321
"""
def Reverse_int(x):
    return (int(''.join(list(str(x))[::-1])))
#x = 123
#print (Reverse_int(x))

"""
Container With Most Water
The above vertical lines are represented by array [1,8,6,2,5,4,8,3,7]. 
In this case, the max area of water (blue section) the container can contain is 49.
L=[1,8,6,2,5,4,8,3,7]
"""

def surface (num1,num2,distance):
    surface=0
    if num1==num2:
        surface=num1*distance
    elif num1>num2:
        surface = num2 * distance
    elif num1 < num2:
        surface = num1 * distance
    return surface

def surface_main (L):
    L_surface=[]
    for index,num in enumerate(L):
        if len(L)-1 == index:
            break
        for index_2 in range(index+1,len(L)):
            next_num=L[index_2]
            distance = index_2 - index #because it starts with 0
            L_surface.append (surface (num,next_num,distance))
    return (max(L_surface))

#L=[1,8,6,2,5,4,8,3,7]
#print (surface_main(L))

"""
6
 Roman to Integer
 Input: s = "III"
Output: 3
Explanation: III = 3.
I             1
V             5
X             10
L             50
C             100
D             500
M             1000
"""

def rome_to_num(char):
    D={'I':1,
       'V':5,
       'X':10,
       'L':50,
       'C':100,
       'D':500,
       'M':100}
    return D[char]

def rome_to_num_main(num):
    L=list(num)
    L= map(rome_to_num,L)
    return (sum(list(L)))

#print (rome_to_num_main('LVIII'))

"""
7
3Sum
Given an integer array nums, return all the triplets 
[nums[i], nums[j], nums[k]] such that
 i != j, i != k, and j != k, and nums[i] + nums[j] + nums[k] == 0
Input: nums = [-1,0,1,2,-1,-4]
Output: [[-1,-1,2],[-1,0,1]]
"""


def check_sum(L):
    if sum(L) == 0:
        return True
    else:
        return False

def Sum3 (L):
    len1= len(L)
    final=[]
    for index,number in enumerate(L):
        L_temp = [number]
        for number_sub in range(index+1,len1):
            L_temp.append(L[number_sub])
            for number_sub_sub in range(number_sub + 1, len1):
                L_temp.append(L[number_sub_sub])
                if (check_sum(L_temp)) == True:
                    final.append(tuple(L_temp))  #because list are connected using pop a trick by me
                L_temp.pop(2)
            L_temp.pop(1)
    return(final)

L=[-1,0,1,2,-1,-4]
print (Sum3(L))


