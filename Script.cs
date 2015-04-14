using System;
using System.IO;

namespace dbcola
{
    public class Script
    {
	private string _content;
	private readonly string _fullFilePath;

	public enum QueryItem
	{
	    Database = 1,
	    Username = 2
	}

	public Script(string a_FilePath)
	{
	    _fullFilePath = a_FilePath;
	    SetContent();
	}

	private void SetContent()
	{
	    _content = File.ReadAllText(_fullFilePath);
	}

	public string GetFileName()
	{
	    return Path.GetFileName(_fullFilePath);
	}

	public string[] Parse()
	{
	    //divide into GO groups to avoid errors
	    var content = _content;

	    content = content.Replace("go\r\n", "GO\r\n");
	    content = content.Replace("go\t", "GO\t");
	    content = content.Replace("\ngo", "\nGO");

	    return content.Split(new[] { "GO\r\n", "GO\t", "\nGO" }, StringSplitOptions.RemoveEmptyEntries);
	}

	public void CustomizeQueryItem(QueryItem a_ItemToCustomize, string a_NewItemContent)
	{
	    switch (a_ItemToCustomize)
	    {
		case QueryItem.Database:
		    _content = _content.Replace("a_database_name", a_NewItemContent);
		    break;
		case QueryItem.Username:
            _content = _content.Replace("a_database_username", a_NewItemContent);
		    break;
	    }
	}

    }
}
