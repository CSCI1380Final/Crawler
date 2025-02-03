/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../config.js');
const util = distribution.util;

test('(1 pts) student test', () => {
  // Fill out this test case...
  const object = {a: undefined, b: null, c: 1, d: "2"};
  const serialized = util.serialize(object);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toEqual(object);
});


test('(1 pts) student test', () => {
  // Fill out this test case...
  const s = ""
  const serialized = util.serialize(s);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toBe(s);
});


test('(1 pts) student test', () => {
  // Fill out this test case...
  const fn = (a) => {return null};
  const serialized = util.serialize(fn);
  const deserialized = util.deserialize(serialized);

  expect(typeof deserialized).toBe('function');
  expect(deserialized(1)).toBeNull();
});

test('(1 pts) student test', () => {
  // Fill out this test case...
  const array = [1, 2, 3, 4, 5, [6, 7, 8, 9, 10], {a: 1, b: 2, c: 3}, new Date()];
  const serialized = util.serialize(array);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toEqual(array);
});

test('(1 pts) student test', () => {
  // Fill out this test case...
  function fn(a) {
    a = a + 1
    return a;
  }
  const serialized = util.serialize(fn);
  const deserialized = util.deserialize(serialized);
  expect(typeof deserialized).toBe('function');
  expect(deserialized(1)).toBe(2);
});
