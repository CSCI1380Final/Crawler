const serialization = require('./serialization'); 

function generateTestCases() {
    return [
        1,
        "This is the test",
        true,
        null,
        undefined,
        [1, 2, 3, 4, 5],
        { name: "zejun"},
        { 
            mix: {
                numbers: [1, 2, 3],
                nested: { a: 1, b: 2 },
                func: function() { return "test"; }
            }
        },
    ];
}

function runLatencyTest(iterations = 1000) {
    const testCases = generateTestCases();
    const serializationTimes = [];
    const deserializationTimes = [];

    testCases.forEach((testCase, index) => {
        console.log(`Test Case ${index + 1}:`);
        const serStartTime = performance.now();
        for (let i = 0; i < iterations; i++) {
            serialization.serialize(testCase);
        }
        const serEndTime = performance.now();
        const serializationTime = (serEndTime - serStartTime) / iterations;
        serializationTimes.push(serializationTime);

        const serialized = serialization.serialize(testCase);
        const deserStartTime = performance.now();
        for (let i = 0; i < iterations; i++) {
            serialization.deserialize(serialized);
        }
        const deserEndTime = performance.now();
        const deserializationTime = (deserEndTime - deserStartTime) / iterations;
        deserializationTimes.push(deserializationTime);
    });

    const avgSerializationTime = 
        serializationTimes.reduce((a, b) => a + b, 0) / serializationTimes.length;
    const avgDeserializationTime = 
        deserializationTimes.reduce((a, b) => a + b, 0) / deserializationTimes.length;

    console.log('\nOverall Performance Summary:');
    console.log(`Average Serialization Time: ${avgSerializationTime.toFixed(6)} ms`);
    console.log(`Average Deserialization Time: ${avgDeserializationTime.toFixed(6)} ms`);
}

runLatencyTest();