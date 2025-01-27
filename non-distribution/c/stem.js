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
  word_list = line.split(' ')
  stem_list = []
  for (let i = 0; i < word_list.length; i++) {
      word = word_list[i]
      stemmedWord = natural.PorterStemmer.stem(word)
      stem_list.push(stemmedWord)
  }
  console.log(stem_list.join(' '))
});
