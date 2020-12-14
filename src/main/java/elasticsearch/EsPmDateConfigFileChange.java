package elasticsearch;

import com.ultrapower.fsms.common.sysconfig.IRedisConfigFileChangeListener;

import java.util.Iterator;
import java.util.Set;


public class EsPmDateConfigFileChange implements IRedisConfigFileChangeListener {

	@Override
	public void configFileChange(Set<String> confFilePathSet) {
		Thread thread= new Thread(){
			@Override
      public void run(){
				doChange(confFilePathSet);
			}
		};
		thread.start();

	}
public void doChange(Set<String> confFilePathSet) {		
		
		
		if(confFilePathSet!=null)
		{
			for (Iterator it = confFilePathSet.iterator(); it.hasNext();) {
				String filename = (String) it.next();
				if (filename.indexOf("pm-storm-rule.properties") > 0) {
					EsIndexDateUtil.getInstance().initPmStoreRule();
				} 

			}
		}

	}

}
