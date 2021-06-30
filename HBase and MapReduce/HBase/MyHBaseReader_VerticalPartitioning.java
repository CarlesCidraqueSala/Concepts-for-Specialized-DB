package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_VerticalPartitioning extends MyHBaseReader {

	protected String[] scanFamilies() {
		/*
		* Només volem llegir una familia, ja que la resposta a la Q1 es troba exclusivament
		* a la familia "Q1TypeRegFlav", per tant retornem un String[] però només amb "Q1TypeRegFlav"
		*
		* Observació: no obtenim el primer valor del primer atribut de la familia, hem fet diferents
		* proves però no aconseguim obtindre el value del primer atribut de la familia.
		* */
		return new String[] {"Q1TypeRegFlav"};
	}
		
}

