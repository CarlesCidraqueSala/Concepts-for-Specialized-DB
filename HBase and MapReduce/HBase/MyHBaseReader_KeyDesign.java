package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_KeyDesign extends MyHBaseReader {
	//s'ha explicat el perquè d'aquest scan en el fitxer MyHBaseWriter_KeyDesign.java
	//scanStart: that returns the starting row key to be used when scanning;
	protected String scanStart() {
		//Row with key type_3-0, és a dir, ha de començar amb type = type_3 i region = 0
		return "type_3-0";
	}
	//scanStop: that returns the ending row key to be used when scanning;
	protected String scanStop() {
		//Row with key type_3-1, és a dir, ha d'acabar amb type = type_3 i region = 1

		/*
		 * En cas de que sapiguèssim que la region 1 es l'última (no es el cas) llavors
		 * no hauriem de posar cap stop, es a dir, retornar null, així el programa
		 * MyHBaseReader.java no faria set de cap stopRow.
		 * */

		return "type_3-1";
	}
		
}
