package org.apache.james.test;

import static org.apache.lucene.util.Version.LUCENE_36;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

public class Indexer {
	private final IndexWriter writer;

	public Indexer(String indexDir) throws IOException {
		final FSDirectory dir = FSDirectory.open(new File(indexDir));
		final IndexWriterConfig config = new IndexWriterConfig(LUCENE_36,
				new StandardAnalyzer(LUCENE_36));
		writer = new IndexWriter(dir, config);
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			throw new IllegalArgumentException("Usage: java "
					+ Indexer.class.getName() + " <index dir> <data dir>");
		}
		String indexDir = args[0];
		String dataDir = args[1];

		long start = System.currentTimeMillis();
		Indexer indexer = new Indexer(indexDir);
		int numIndexed;
		try {
			numIndexed = indexer.index(dataDir, new TextFilesFilter());
		} finally {
			indexer.close();
		}
		long end = System.currentTimeMillis();
		System.out.println("Indexing " + numIndexed + " files took "
				+ (end - start) + "milliseconds");
	}

	private int index(String dataDir, TextFilesFilter filter)
			throws IOException {
		File[] files = new File(dataDir).listFiles();
		for (File f : files)
			if (!f.isDirectory() && !f.isHidden() && f.exists() && f.canRead()
					&& (filter == null || filter.accept(f)))
				indexFile(f);
		return writer.numDocs();
	}

	private void close() throws IOException {
		writer.close();
	}

	protected Document getDocument(File f) throws IOException {
		Document doc = new Document();
		doc.add(new Field("contents", new FileReader(f)));
		doc.add(new Field("filename", f.getName(), Field.Store.YES,
				Field.Index.NOT_ANALYZED));
		doc.add(new Field("fullpath", f.getCanonicalPath(), Field.Store.YES,
				Field.Index.NOT_ANALYZED));
		return doc;
	}

	private void indexFile(File f) throws IOException {
		System.out.println("Indexing " + f.getCanonicalPath());
		Document doc = getDocument(f);
		writer.addDocument(doc);
	}

	private static class TextFilesFilter implements FileFilter {

		public boolean accept(File pathname) {
			return pathname.getName().toLowerCase().endsWith(".txt");
		}
	}
}
