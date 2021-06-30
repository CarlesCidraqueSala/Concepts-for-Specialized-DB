package cbde.labs.hbase_mapreduce.writer;
/*
* [1.1] Create a new table with the best layout to accommodate such query.
* Write as a comment the create statement in MyHBaseWriter_VerticalPartitioning.java :
* -------------------------------------------
* create 'wines2', 'Q1TypeRegFlav', 'Others'
* -------------------------------------------
* (Justificació: creem obviament només una taula per consulta; posem tots els atributs
* consultats en la Q1 en una mateixa family (Q1TypeRegFlav) ja que les families son
* fragmentació vertical, físicament tenim tants fitxers com families, es guardarà en
* cada familia la PK amb el seu timestamp i els atributs de la familia. Fer l'affinity matrix
* té sentit, hem de posar atributs en una mateixa familia si tenen alta affinitat, en aquest
* cas com es obvi al només haverhi una query aquest atributs ("type","flav" i "region") sempre
* es consulten a la vegada, per tant els ubiquem en una mateixa familia. Tots els restants van
* a la familia "Others")
 * */
public class MyHBaseWriter_VerticalPartitioning extends MyHBaseWriter {

	protected String toFamily(String attribute) {
		/*
		* Tal i com hem comentat en la justificació anterior creem dos families.
		* Quan es cridi a la funció toFamily amb un determinat atribut desde el programa
		* MyHBaseWriter.java amb aquesta funció retornem a la familia en la que ha d'anar
		* aquest atribut pasat com a paràmetre.
		* */
		if(attribute == "flav" || attribute == "type" || attribute == "region") {
			return "Q1TypeRegFlav";
		}
		else return "Others";

	}
		
}
