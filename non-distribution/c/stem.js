#!/usr/bin/env node

/*
Convert each term to its stem
Usage: ./stem.js <input >output
*/

const readline = require('readline');
const natural = require('natural');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false,
});

rl.on('line', function(line) {
  // Print the Porter stem from `natural` for each element of the stream.
  const wordList = line.split(' ');
  const stemList = [];
  for (let i = 0; i < wordList.length; i++) {
    const word = wordList[i];
    const stemmedWord = natural.PorterStemmer.stem(word);
    stemList.push(stemmedWord);
  }
  console.log(stemList.join(' '));
});
