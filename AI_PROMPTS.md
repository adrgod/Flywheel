
---
My spark code is failing with java, help me set it up.

(many iteractions so sort java environment issues and conda virtual env issues)

---
do I need to add anything about java runtime in requirements.txt so anyone running this code will see it working and not go throught this trouble?


---
event_timestamp format in output parquet is like "1705307400000" is this expected? or should I show a human readable format?


---
where to apply a date formatting early in my code, so both json and csv are affected?


(more prompt to help sorting code issues)

---
can the environment.yml file point (use) requirements.txt so if there's a change in requirement I don't have to change in local deployment and in docker?

---
 where's the best place to add pandas calculations? should I read spark output or perform calculations in-transit in spark?


---
what are the engines spark can use and what are their pros and cons?

(a few more prompt regarding docker mount to read and write the data to local folders)

---
my container image is 2.8 Gb big. how can I reduze its size?



---
if I wanted to partition the input data by a field, what's the best way to do it?


---
how to mock a spark session for unit tests?

(some more prompts to have help writing unit tests)


--
is it possible to flat out the json so every data entries are read, no matter if it's nested or what its element strucutre is?

---
what if tomorrow a new file comes in and has a different format? can I have a generic flattening of json elements?
