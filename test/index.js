const { Storage } = require('@google-cloud/storage');

const TEST_BUCKETNAME = 'some-bucket'
const TEST_FILENAME = 'nested/file/foo.txt'

const storage = new Storage({
	apiEndpoint: 'http://localhost:8000'
});

const bucket = storage.bucket(TEST_BUCKETNAME)

async function main() {
	console.log('initialising...')
	const file = bucket.file(TEST_FILENAME)
	try {
		const [buf] = await file.download()
		const contents = buf.toString('utf-8')
		console.log({ contents })
		contents += `\m${Math.random()}`
		await file.save(contents)
	} catch (err) {
		// todo: unable to validate - should be good *enough*
		await file.save('line1', { resumable: false, validation: false })
	}
}

void main()
