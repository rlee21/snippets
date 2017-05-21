import textwrap



def wrap(text, width):
    reflow = textwrap.wrap(text, width=40)
    for i in reflow:
        print(i)
    

 


if __name__ == "__main__":
     
    text = """In the beginning God created the heavens and the earth. Now the earth was 
    formless and empty, darkness was over the surface of the deep, and the Spirit of
    God was hovering over the waters.
    
    And God said, "Let there be light," and there was light. God saw that the light
    was good, and he separated the light from the darkness. God called the light
    "day," and the darkness he called "night." And there was evening, and there was
    morning - the first day."""
    wrap(text, 40)
