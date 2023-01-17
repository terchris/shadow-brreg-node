# shadow-brreg-node
typescript/node code for accessing the postgres shadow copy of brreg.no (brønnøysundregistrene) 



I have used ChatGPT to set up development environment, getting debugging in vscode working, typescript working. All code is written by ChatGPT.

I have added all my questions and ChatGPT's answers to the document charGPT.MD
As I "talk" to ChatGPT and develop the code I do commit so that it is possible to see how ChatGPT has guided my coding. 

## how to play
To play with we need lots of data. I have created a docker image that download and creates a database that has all companies and organizations in Norway. About a milion (small country) organizations in the database.
https://github.com/terchris/shadow-brreg
