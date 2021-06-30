package cbde.labs.hbase_mapreduce.writer;

import java.util.Set;

public class MyHBaseWriter_KeyDesign extends MyHBaseWriter {
/*
*Creem una key composta (type, region) per evitar scans innecessaris, per evitar posibles
* table scans. Ens interesa una cosa molt concreta, type = type_3 and region = 0. fent-ho així
* ens facilitarà posteriorment els scans a fer per aquesta query.
*
* Veiem que hi ha valors, per exemple el de la query (type = type_3 i region = 0) que pot ser
* que no es crein, ja que la creació es fa en moment d'incerció. Per tant, poden haver-hi salts
* entre els valors d'un atribut i un altre, però sempre ordenats (les fulles del B+),
* bàsic per fer l'scan.
* */
	protected String nextKey() {
		return this.data.get("type") + '-' + this.data.get("region");
	}

}
